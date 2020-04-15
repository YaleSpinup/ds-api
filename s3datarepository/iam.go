package s3datarepository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/YaleSpinup/ds-api/apierror"
	"github.com/YaleSpinup/ds-api/dataset"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/aws/aws-sdk-go/service/sts"
	log "github.com/sirupsen/logrus"
)

// PolicyStatement is an individual IAM Policy statement
type PolicyStatement struct {
	Effect    string
	Action    []string
	Resource  []string            `json:",omitempty"`
	Principal map[string][]string `json:",omitempty"`
}

// PolicyDoc collects the policy statements
type PolicyDoc struct {
	Version   string
	Statement []PolicyStatement
}

// createPolicy creates the appropriate access policy for the data repository, depending if it's a derivative or not
func (s *S3Repository) createPolicy(ctx context.Context, id string, derivative bool) error {
	if id == "" {
		return apierror.New(apierror.ErrBadRequest, "invalid input", errors.New("empty id"))
	}

	name := id
	if s.NamePrefix != "" {
		name = s.NamePrefix + "-" + name
	}

	policyName := fmt.Sprintf("policy-%s", name)

	var policyDoc []byte
	var err error

	log.Debugf("generating access policy for bucket '%s'", name)

	if derivative {
		policyDoc, err = s.derivativeAccessPolicy(name)
	} else {
		policyDoc, err = s.originalAccessPolicy(name)
	}
	if err != nil {
		return ErrCode("failed to generate IAM policy for bucket "+name, err)
	}

	log.Debugf("creating access policy for bucket '%s'", name)

	// create policy
	policyOutput, err := s.IAM.CreatePolicyWithContext(ctx, &iam.CreatePolicyInput{
		Description:    aws.String(fmt.Sprintf("Access policy for dataset bucket %s", name)),
		Path:           aws.String(s.IAMPathPrefix),
		PolicyDocument: aws.String(string(policyDoc)),
		PolicyName:     aws.String(policyName),
	})
	if err != nil {
		return ErrCode("failed to create IAM policy", err)
	}

	log.Debugf("created policy: %s", *policyOutput.Policy.Arn)

	return nil
}

// createRole creates a role and instance profile for an instance to access a data set
// returns a slice of functions to perform rollback of its actions
func (s *S3Repository) createRole(ctx context.Context, roleName, instanceID string) ([]func() error, error) {
	var rollBackTasks []func() error

	log.Debugf("creating role %s", roleName)

	roleDoc, err := s.assumeRolePolicy()
	if err != nil {
		return rollBackTasks, ErrCode("failed to generate IAM assume role policy", err)
	}

	var roleOutput *iam.CreateRoleOutput
	if roleOutput, err = s.IAM.CreateRoleWithContext(ctx, &iam.CreateRoleInput{
		AssumeRolePolicyDocument: aws.String(string(roleDoc)),
		Description:              aws.String(fmt.Sprintf("Role for instance %s", instanceID)),
		Path:                     aws.String(s.IAMPathPrefix),
		RoleName:                 aws.String(roleName),
	}); err != nil {
		return rollBackTasks, ErrCode("failed to create IAM role "+roleName, err)
	}

	// append role delete to rollback tasks
	rollBackTasks = append(rollBackTasks, func() error {
		return func() error {
			log.Debug("DeleteRoleWithContext")
			if _, err := s.IAM.DeleteRoleWithContext(ctx, &iam.DeleteRoleInput{RoleName: roleOutput.Role.RoleName}); err != nil {
				return err
			}
			return nil
		}()
	})

	log.Debugf("creating instance profile %s", roleName)

	var instanceProfileOutput *iam.CreateInstanceProfileOutput
	if instanceProfileOutput, err = s.IAM.CreateInstanceProfileWithContext(ctx, &iam.CreateInstanceProfileInput{
		InstanceProfileName: aws.String(roleName),
		Path:                aws.String(s.IAMPathPrefix),
	}); err != nil {
		return rollBackTasks, ErrCode("failed to create instance profile "+roleName, err)
	}

	// append instance profile delete to rollback tasks
	rollBackTasks = append(rollBackTasks, func() error {
		return func() error {
			log.Debug("DeleteInstanceProfileWithContext")
			if _, err := s.IAM.DeleteInstanceProfileWithContext(ctx, &iam.DeleteInstanceProfileInput{InstanceProfileName: aws.String(roleName)}); err != nil {
				return err
			}
			return nil
		}()
	})

	log.Debugf("adding role to instance profile %s", roleName)

	if _, err = s.IAM.AddRoleToInstanceProfileWithContext(ctx, &iam.AddRoleToInstanceProfileInput{
		InstanceProfileName: aws.String(roleName),
		RoleName:            aws.String(roleName),
	}); err != nil {
		return rollBackTasks, ErrCode("failed to add role to instance profile "+roleName, err)
	}

	// append role removal from instance profile to rollback tasks
	rollBackTasks = append(rollBackTasks, func() error {
		return func() error {
			log.Debug("RemoveRoleFromInstanceProfileWithContext")
			if _, err := s.IAM.RemoveRoleFromInstanceProfileWithContext(ctx, &iam.RemoveRoleFromInstanceProfileInput{
				InstanceProfileName: aws.String(roleName),
				RoleName:            aws.String(roleName),
			}); err != nil {
				return err
			}
			return nil
		}()
	})

	log.Debugf("created instance profile: %s", aws.StringValue(instanceProfileOutput.InstanceProfile.Arn))

	return rollBackTasks, nil
}

// deletePolicy deletes the access policy for the given data repository
func (s *S3Repository) deletePolicy(ctx context.Context, id string) error {
	if id == "" {
		return apierror.New(apierror.ErrBadRequest, "invalid input", errors.New("empty id"))
	}

	name := id
	if s.NamePrefix != "" {
		name = s.NamePrefix + "-" + name
	}

	policyName := fmt.Sprintf("policy-%s", name)

	policyArn, err := s.getPolicyArn(ctx, policyName)
	if err != nil {
		return ErrCode("failed to get ARN for policy "+policyName, err)
	}

	// TODO: check if policy is used anywhere before deleting

	if _, err = s.IAM.DeletePolicyWithContext(ctx, &iam.DeletePolicyInput{PolicyArn: aws.String(policyArn)}); err != nil {
		return ErrCode("failed to delete policy "+policyArn, err)
	}

	log.Debugf("deleted policy: %s", policyArn)

	return nil
}

// getPolicyArn constructs the policy ARN from the policy name
func (s *S3Repository) getPolicyArn(ctx context.Context, name string) (string, error) {
	if name == "" {
		return "", apierror.New(apierror.ErrBadRequest, "invalid input", errors.New("empty name"))
	}

	// prepend the path prefix
	if s.IAMPathPrefix == "" {
		name = "/" + name
	} else {
		name = s.IAMPathPrefix + name
	}

	log.Debugf("constructing ARN for policy %s", name)

	callerID, err := s.STS.GetCallerIdentityWithContext(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return "", err
	}

	policyArn := fmt.Sprintf("arn:aws:iam::%s:policy%s", *callerID.Account, name)

	log.Debugf("policy ARN: %s", policyArn)

	return policyArn, nil
}

// ListAccess lists all instances that have access to the data repository
// Returns a map with the instance id's and their assigned instance profile,
// e.g. { "instance_id": "instance_profile_name" }
func (s *S3Repository) ListAccess(ctx context.Context, id string) (dataset.Access, error) {
	if id == "" {
		return nil, apierror.New(apierror.ErrBadRequest, "invalid input", errors.New("empty id"))
	}

	name := id
	if s.NamePrefix != "" {
		name = s.NamePrefix + "-" + name
	}

	log.Infof("listing instances with access to s3datarepository %s", name)

	policyName := fmt.Sprintf("policy-%s", name)

	policyArn, err := s.getPolicyArn(ctx, policyName)
	if err != nil {
		return nil, ErrCode("failed to get ARN for policy "+policyName, err)
	}

	log.Debugf("listing roles with policy %s", policyArn)

	// find out what roles the policy is attached to
	entitiesOut, err := s.IAM.ListEntitiesForPolicyWithContext(ctx, &iam.ListEntitiesForPolicyInput{
		EntityFilter:      aws.String("Role"),
		PathPrefix:        aws.String(s.IAMPathPrefix),
		PolicyArn:         aws.String(policyArn),
		PolicyUsageFilter: aws.String("PermissionsPolicy"),
	})
	if err != nil {
		return nil, ErrCode("failed to list entities for policy "+policyArn, err)
	}

	log.Debug(entitiesOut.PolicyRoles)

	if len(entitiesOut.PolicyRoles) == 0 {
		log.Infof("policy %s is not attached to any roles", policyName)
	}

	output := dataset.Access{}

	// find out what instances each role is assigned to
	for _, r := range entitiesOut.PolicyRoles {
		roleName := aws.StringValue(r.RoleName)

		ipOut, err := s.IAM.ListInstanceProfilesForRoleWithContext(ctx, &iam.ListInstanceProfilesForRoleInput{
			RoleName: r.RoleName,
		})
		if err != nil {
			return nil, ErrCode("failed to list instance profiles for role "+roleName, err)
		}

		if len(ipOut.InstanceProfiles) == 0 {
			log.Warnf("role is not associated with any instance profiles: %s", roleName)
			continue
		}

		instanceProfileArn := make([]*string, 0, len(ipOut.InstanceProfiles))
		instanceProfileName := make([]string, 0, len(ipOut.InstanceProfiles))
		for _, ip := range ipOut.InstanceProfiles {
			instanceProfileArn = append(instanceProfileArn, ip.Arn)
			instanceProfileName = append(instanceProfileName, aws.StringValue(ip.InstanceProfileName))
		}

		log.Debugf("listing instances with instance profile: %s", strings.Join(instanceProfileName, ","))

		instancesOut, err := s.EC2.DescribeInstances(&ec2.DescribeInstancesInput{
			Filters: []*ec2.Filter{
				{
					Name:   aws.String("iam-instance-profile.arn"),
					Values: instanceProfileArn,
				},
			},
		})
		if err != nil {
			return nil, ErrCode("failed to list instances with instance profile "+strings.Join(instanceProfileName, ","), err)
		}

		if len(instancesOut.Reservations) == 0 {
			log.Warnf("instance profile is not assigned to any instances: %s", strings.Join(instanceProfileName, ","))
			continue
		}

		for _, reservation := range instancesOut.Reservations {
			for _, instance := range reservation.Instances {
				log.Debugf("found instance %v", aws.StringValue(instance.InstanceId))

				// we only have the instance profile arn, so let's extract the name
				ipArns := strings.Split(aws.StringValue(instance.IamInstanceProfile.Arn), "/")
				instanceProfileName := ipArns[len(ipArns)-1]

				output[aws.StringValue(instance.InstanceId)] = instanceProfileName
			}
		}
	}

	return output, nil
}

// GrantAccess gives an instance access to the data repository by setting up a role (instance profile)
// If the instance already has an associated instance profile, it will copy all of its policies to
// the new instance profile and swap out the profiles
// Returns the instance id and the arn of the instance profile
func (s *S3Repository) GrantAccess(ctx context.Context, id, instanceID string) (dataset.Access, error) {
	if id == "" {
		return nil, apierror.New(apierror.ErrBadRequest, "invalid input", errors.New("empty id"))
	}

	if instanceID == "" {
		return nil, apierror.New(apierror.ErrBadRequest, "invalid input", errors.New("empty instanceID"))
	}

	name := id
	if s.NamePrefix != "" {
		name = s.NamePrefix + "-" + name
	}

	log.Infof("granting instance %s access to s3datarepository %s", instanceID, name)

	policyName := fmt.Sprintf("policy-%s", name)

	policyArn, err := s.getPolicyArn(ctx, policyName)
	if err != nil {
		return nil, ErrCode("failed to get ARN for policy "+policyName, err)
	}

	log.Debugf("getting information about instance %s", instanceID)

	// we describe the given instance so we can
	// 1) make sure it exists, and 2) see if it already has an instance profile association
	instancesOut, err := s.EC2.DescribeInstances(&ec2.DescribeInstancesInput{
		InstanceIds: []*string{aws.String(instanceID)},
	})
	if err != nil {
		return nil, ErrCode("failed to get information about instance "+instanceID, err)
	}

	if len(instancesOut.Reservations) == 0 || len(instancesOut.Reservations[0].Instances) == 0 {
		return nil, ErrCode("could not find instance "+instanceID, err)
	}

	if len(instancesOut.Reservations) > 1 || len(instancesOut.Reservations[0].Instances) > 1 {
		return nil, ErrCode("more than one match found for instance "+instanceID, err)
	}

	instanceInfo := instancesOut.Reservations[0].Instances[0]

	// setup rollback function list and defer execution
	var rollBackTasks []func() error
	defer func() {
		if err != nil {
			log.Errorf("recovering from error granting access to s3datarepository: %s, executing %d rollback tasks", err, len(rollBackTasks))
			rollBack(&rollBackTasks)
		}
	}()

	// the instance role name is programmatically determined
	// it is equivalent to the instance profile name
	roleName := fmt.Sprintf("instanceRole_%s", instanceID)

	roleExists := true
	if _, err = s.IAM.GetRole(&iam.GetRoleInput{RoleName: aws.String(roleName)}); err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == iam.ErrCodeNoSuchEntityException {
				roleExists = false
				log.Debugf("role %s does not exist", roleName)
			} else {
				return nil, ErrCode("failed to get IAM role "+roleName, err)
			}
		} else {
			return nil, ErrCode("failed to get IAM role "+roleName, err)
		}
	} else {
		log.Debugf("role %s already exists", roleName)
	}

	// if there's no existing role for this instance we'll create one and associate it with an
	// instance profile with the same name
	// we also assume that if the role exists, its corresponding instance profile already exists,
	// which will be true unless manually modified outside of this api
	if !roleExists {
		createRoleRollback, err := s.createRole(ctx, roleName, instanceID)
		rollBackTasks = append(rollBackTasks, createRoleRollback...)
		if err != nil {
			return nil, err
		}
	}

	log.Debugf("attaching policy %s to role %s", policyArn, roleName)

	_, err = s.IAM.AttachRolePolicyWithContext(ctx, &iam.AttachRolePolicyInput{
		PolicyArn: aws.String(policyArn),
		RoleName:  aws.String(roleName),
	})
	if err != nil {
		return nil, ErrCode("failed to attach policy "+policyArn+" to role "+roleName, err)
	}

	// append policy detach from role to rollback tasks
	rollBackTasks = append(rollBackTasks, func() error {
		return func() error {
			log.Debug("DetachRolePolicyWithContext")
			if _, err := s.IAM.DetachRolePolicyWithContext(ctx, &iam.DetachRolePolicyInput{
				PolicyArn: aws.String(policyArn),
				RoleName:  aws.String(roleName),
			}); err != nil {
				return err
			}
			return nil
		}()
	})

	var instanceRoleAssociated bool

	// if the instance already has some other instance profile, we copy all policies
	// from the existing profile into the new one and then disassociate the old profile
	if instanceInfo.IamInstanceProfile != nil {
		// we only have the instance profile arn, so let's extract the name
		ipArns := strings.Split(aws.StringValue(instanceInfo.IamInstanceProfile.Arn), "/")
		currentInstanceProfileName := ipArns[len(ipArns)-1]
		if currentInstanceProfileName == roleName {
			instanceRoleAssociated = true
		}

		if !instanceRoleAssociated {
			log.Infof("instance %s already has instance profile %s, will try to migrate existing policies", instanceID, currentInstanceProfileName)

			// find out what role(s) correspond to this instance profile and what policies are attached to them
			var ipOut *iam.GetInstanceProfileOutput
			if ipOut, err = s.IAM.GetInstanceProfileWithContext(ctx, &iam.GetInstanceProfileInput{
				InstanceProfileName: aws.String(currentInstanceProfileName),
			}); err != nil {
				return nil, ErrCode("failed to get information about current instance profile "+currentInstanceProfileName, err)
			}

			// TODO: we are _not_ considering role inline policies at this point, should we?
			var currentPoliciesArn []string
			for _, r := range ipOut.InstanceProfile.Roles {
				log.Debugf("listing attached policies for role %s", aws.StringValue(r.RoleName))

				var attachedRolePoliciesOut *iam.ListAttachedRolePoliciesOutput
				if attachedRolePoliciesOut, err = s.IAM.ListAttachedRolePoliciesWithContext(ctx, &iam.ListAttachedRolePoliciesInput{
					RoleName: r.RoleName,
				}); err != nil {
					return nil, ErrCode("failed to list attached policies for role "+aws.StringValue(r.RoleName), err)
				}

				if attachedRolePoliciesOut.AttachedPolicies == nil {
					log.Warnf("no attached policies found for current role %s, there may be inline policies", aws.StringValue(r.RoleName))
				}

				for _, p := range attachedRolePoliciesOut.AttachedPolicies {
					currentPoliciesArn = append(currentPoliciesArn, aws.StringValue(p.PolicyArn))
				}
			}

			log.Infof("policies attached to the current instance profile: %s", currentPoliciesArn)

			// attach current policies to our new role
			for _, p := range currentPoliciesArn {
				log.Debugf("attaching pre-existing policy %s to role %s", p, roleName)

				_, err = s.IAM.AttachRolePolicyWithContext(ctx, &iam.AttachRolePolicyInput{
					PolicyArn: aws.String(p),
					RoleName:  aws.String(roleName),
				})
				if err != nil {
					return nil, ErrCode("failed to attach policy "+p+" to role "+roleName, err)
				}
			}

			// append policy detach from role to rollback tasks
			rollBackTasks = append(rollBackTasks, func() error {
				return func() error {
					for _, p := range currentPoliciesArn {
						log.Debugf("DetachRolePolicyWithContext: %s (%s)", p, roleName)
						err = retry(3, 3*time.Second, func() error {
							_, err = s.IAM.DetachRolePolicyWithContext(ctx, &iam.DetachRolePolicyInput{
								PolicyArn: aws.String(p),
								RoleName:  aws.String(roleName),
							})
							if err != nil {
								log.Debugf("retrying, got error: %s", err)
								return err
							}
							return nil
						})
						if err != nil {
							log.Warnf("failed to detach policy "+p+" from role "+roleName, err)
							continue
						}
					}
					return nil
				}()
			})

			// find out the association id for the currently associated instance profile
			var ipAssociationsOut *ec2.DescribeIamInstanceProfileAssociationsOutput
			if ipAssociationsOut, err = s.EC2.DescribeIamInstanceProfileAssociationsWithContext(ctx, &ec2.DescribeIamInstanceProfileAssociationsInput{
				Filters: []*ec2.Filter{
					{
						Name:   aws.String("instance-id"),
						Values: []*string{aws.String(instanceID)},
					},
					{
						Name:   aws.String("state"),
						Values: []*string{aws.String("associated")},
					},
				},
			}); err != nil {
				return nil, ErrCode("failed to describe instance profile associations for instance "+instanceID, err)
			}

			log.Debugf("got associations: %+v", ipAssociationsOut.IamInstanceProfileAssociations)

			if len(ipAssociationsOut.IamInstanceProfileAssociations) != 1 {
				return nil, ErrCode("did not find exactly 1 instance profile association for instance "+instanceID, nil)
			}

			log.Debugf("disassociating association id %s", aws.StringValue(ipAssociationsOut.IamInstanceProfileAssociations[0].AssociationId))

			// retry the instance profile disassociation
			err = retry(5, 3*time.Second, func() error {
				_, err = s.EC2.DisassociateIamInstanceProfileWithContext(ctx, &ec2.DisassociateIamInstanceProfileInput{
					AssociationId: ipAssociationsOut.IamInstanceProfileAssociations[0].AssociationId,
				})
				if err != nil {
					log.Debugf("retrying, got error: %s", err)
					return err
				}
				return nil
			})
			if err != nil {
				return nil, ErrCode("failed to disassociate current instance profile from instance "+instanceID, err)
			}

			// append original instance profile association to rollback tasks
			rollBackTasks = append(rollBackTasks, func() error {
				return func() error {
					log.Debug("AssociateIamInstanceProfileWithContext")
					err = retry(5, 3*time.Second, func() error {
						_, err = s.EC2.AssociateIamInstanceProfileWithContext(ctx, &ec2.AssociateIamInstanceProfileInput{
							IamInstanceProfile: &ec2.IamInstanceProfileSpecification{
								Arn: instanceInfo.IamInstanceProfile.Arn,
							},
							InstanceId: aws.String(instanceID),
						})
						if err != nil {
							log.Debugf("retrying, got error: %s", err)
							return err
						}
						return nil
					})
					if err != nil {
						return err
					}
					return nil
				}()
			})
		}
	}

	// we associate the new instance profile with the instance, unless it's already associated
	if !instanceRoleAssociated {
		log.Infof("associating instance profile %s with instance %s", roleName, instanceID)

		// retry the instance profile association as it takes a while to show up
		err = retry(5, 3*time.Second, func() error {
			_, err = s.EC2.AssociateIamInstanceProfileWithContext(ctx, &ec2.AssociateIamInstanceProfileInput{
				IamInstanceProfile: &ec2.IamInstanceProfileSpecification{
					Name: aws.String(roleName),
				},
				InstanceId: aws.String(instanceID),
			})
			if err != nil {
				log.Debugf("retrying, got error: %s", err)
				return err
			}
			return nil
		})
		if err != nil {
			return nil, ErrCode("failed to associate instance profile with instance "+instanceID, err)
		}

		log.Debugf("associated instance profile %s with instance %s", roleName, instanceID)
	}

	output := dataset.Access{
		instanceID: roleName,
	}

	return output, nil
}

// RevokeAccess revokes instance access from the data repository by
// removing the dataset access policy from the instance profile (role)
// Note this will leave the instance role in place, since it may contain other policies
func (s *S3Repository) RevokeAccess(ctx context.Context, id, instanceID string) error {
	if id == "" {
		return apierror.New(apierror.ErrBadRequest, "invalid input", errors.New("empty id"))
	}

	if instanceID == "" {
		return apierror.New(apierror.ErrBadRequest, "invalid input", errors.New("empty instanceID"))
	}

	name := id
	if s.NamePrefix != "" {
		name = s.NamePrefix + "-" + name
	}

	log.Infof("revoking instance %s access from s3datarepository %s", instanceID, name)

	policyName := fmt.Sprintf("policy-%s", name)

	policyArn, err := s.getPolicyArn(ctx, policyName)
	if err != nil {
		return ErrCode("failed to get ARN for policy "+policyName, err)
	}

	log.Debugf("getting information about instance %s", instanceID)

	// we describe the given instance so we can
	// 1) make sure it exists, and 2) see if it already has an instance profile association
	instancesOut, err := s.EC2.DescribeInstances(&ec2.DescribeInstancesInput{
		InstanceIds: []*string{aws.String(instanceID)},
	})
	if err != nil {
		return ErrCode("failed to get information about instance "+instanceID, err)
	}

	if len(instancesOut.Reservations) == 0 || len(instancesOut.Reservations[0].Instances) == 0 {
		return ErrCode("could not find instance "+instanceID, err)
	}

	if len(instancesOut.Reservations) > 1 || len(instancesOut.Reservations[0].Instances) > 1 {
		return ErrCode("more than one match found for instance "+instanceID, err)
	}

	instanceInfo := instancesOut.Reservations[0].Instances[0]

	if instanceInfo.IamInstanceProfile == nil {
		log.Warnf("instance %s does not have an associated instance profile", instanceID)
		return apierror.New(apierror.ErrBadRequest, "invalid input", errors.New("instance "+instanceID+" does not have access to dataset"))
	}

	log.Debugf("instance %s has instance profile %s", instanceID, aws.StringValue(instanceInfo.IamInstanceProfile.Arn))

	// we only have the instance profile arn, so let's extract the name
	ipArns := strings.Split(aws.StringValue(instanceInfo.IamInstanceProfile.Arn), "/")
	currentInstanceProfileName := ipArns[len(ipArns)-1]

	// find out what role(s) correspond to this instance profile and what policies are attached to them
	var ipOut *iam.GetInstanceProfileOutput
	if ipOut, err = s.IAM.GetInstanceProfileWithContext(ctx, &iam.GetInstanceProfileInput{
		InstanceProfileName: aws.String(currentInstanceProfileName),
	}); err != nil {
		return ErrCode("failed to get information about current instance profile "+currentInstanceProfileName, err)
	}

	var policyFound bool

	// find the dataset access policy and detach it from the role
	for _, r := range ipOut.InstanceProfile.Roles {
		log.Debugf("listing attached policies for role %s", aws.StringValue(r.RoleName))

		var attachedRolePoliciesOut *iam.ListAttachedRolePoliciesOutput
		if attachedRolePoliciesOut, err = s.IAM.ListAttachedRolePoliciesWithContext(ctx, &iam.ListAttachedRolePoliciesInput{
			RoleName: r.RoleName,
		}); err != nil {
			return ErrCode("failed to list attached policies for role "+aws.StringValue(r.RoleName), err)
		}

		for _, p := range attachedRolePoliciesOut.AttachedPolicies {
			if aws.StringValue(p.PolicyArn) == policyArn {
				policyFound = true
				log.Debugf("detaching dataset access policy %s from role %s", policyArn, aws.StringValue(r.RoleName))

				_, err = s.IAM.DetachRolePolicyWithContext(ctx, &iam.DetachRolePolicyInput{
					PolicyArn: p.PolicyArn,
					RoleName:  r.RoleName,
				})
				if err != nil {
					return ErrCode("failed to detach policy "+aws.StringValue(p.PolicyArn)+" from role "+aws.StringValue(r.RoleName), err)
				}

				break
			}
		}
	}

	if !policyFound {
		log.Warnf("did not find dataset access policy %s in any of the roles associated with this instance %s", policyArn, instanceID)
		return apierror.New(apierror.ErrBadRequest, "invalid input", errors.New("instance "+instanceID+" does not have access to dataset"))
	}

	return nil
}

// GrantTemporaryAccess sets up temporary (user) access to the repository
// TODO: Finish this
func (s *S3Repository) GrantTemporaryAccess(ctx context.Context, id string) (*dataset.Access, error) {
	if id == "" {
		return nil, apierror.New(apierror.ErrBadRequest, "invalid input", errors.New("empty id"))
	}

	name := id
	if s.NamePrefix != "" {
		name = s.NamePrefix + "-" + name
	}

	log.Debugf("granting temporary access to s3datarepository: %s", name)

	policyName := fmt.Sprintf("%s-TempPlc", name)
	policyDoc, err := s.temporaryAccessPolicy(name)
	if err != nil {
		return nil, ErrCode("failed to generate temporary IAM policy for bucket "+name, err)
	}

	log.Debugf("creating temporary access policy for bucket '%s'", name)

	// setup rollback function list and defer execution
	var rollBackTasks []func() error
	defer func() {
		if err != nil {
			log.Errorf("recovering from error granting access to s3datarepository: %s, executing %d rollback tasks", err, len(rollBackTasks))
			rollBack(&rollBackTasks)
		}
	}()

	policyOutput, err := s.IAM.CreatePolicyWithContext(ctx, &iam.CreatePolicyInput{
		Description:    aws.String(fmt.Sprintf("Temporary policy for bucket %s", name)),
		Path:           aws.String(s.IAMPathPrefix),
		PolicyDocument: aws.String(string(policyDoc)),
		PolicyName:     aws.String(policyName),
	})
	if err != nil {
		return nil, ErrCode("failed to create temporary IAM policy", err)
	}

	// append policy delete to rollback tasks
	rollBackTasks = append(rollBackTasks, func() error {
		return func() error {
			if _, err := s.IAM.DeletePolicyWithContext(ctx, &iam.DeletePolicyInput{PolicyArn: policyOutput.Policy.Arn}); err != nil {
				return err
			}
			return nil
		}()
	})

	// TODO: create group/user and attach policy, generate key

	output := &dataset.Access{}

	return output, nil
}

// assumeRolePolicy defines the IAM policy for assuming a role
func (s *S3Repository) assumeRolePolicy() ([]byte, error) {
	policyDoc, err := json.Marshal(PolicyDoc{
		Version: "2012-10-17",
		Statement: []PolicyStatement{
			PolicyStatement{
				Effect: "Allow",
				Action: []string{"sts:AssumeRole"},
				Principal: map[string][]string{
					"Service": {"ec2.amazonaws.com"},
				},
			},
		}})
	if err != nil {
		log.Errorf("failed to generate assume role policy: %s", err)
		return []byte{}, err
	}

	log.Debugf("generated assume role policy with document %s", string(policyDoc))

	return policyDoc, nil
}

// derivativeAccessPolicy defines the IAM policy for access to a derivative dataset (RW)
func (s *S3Repository) derivativeAccessPolicy(bucket string) ([]byte, error) {
	log.Debugf("generating derivative bucket access policy for %s", bucket)

	policyDoc, err := json.Marshal(PolicyDoc{
		Version: "2012-10-17",
		Statement: []PolicyStatement{
			PolicyStatement{
				Resource: []string{fmt.Sprintf("arn:aws:s3:::%s", bucket)},
				Effect:   "Allow",
				Action:   []string{"s3:ListBucket"},
			},
			PolicyStatement{
				Resource: []string{fmt.Sprintf("arn:aws:s3:::%s/*", bucket)},
				Effect:   "Allow",
				Action: []string{
					"s3:DeleteObject",
					"s3:GetObject",
					"s3:PutObject",
				},
			},
		},
	})

	if err != nil {
		log.Errorf("failed to generate derivative bucket access policy for %s: %s", bucket, err)
		return []byte{}, err
	}

	log.Debugf("generated policy with document %s", string(policyDoc))

	return policyDoc, nil
}

// originalAccessPolicy defines the IAM policy for access to an original dataset (RO)
func (s *S3Repository) originalAccessPolicy(bucket string) ([]byte, error) {
	log.Debugf("generating original bucket access policy for %s", bucket)

	policyDoc, err := json.Marshal(PolicyDoc{
		Version: "2012-10-17",
		Statement: []PolicyStatement{
			PolicyStatement{
				Resource: []string{fmt.Sprintf("arn:aws:s3:::%s", bucket)},
				Effect:   "Allow",
				Action:   []string{"s3:ListBucket"},
			},
			PolicyStatement{
				Resource: []string{fmt.Sprintf("arn:aws:s3:::%s/*", bucket)},
				Effect:   "Allow",
				Action:   []string{"s3:GetObject"},
			},
		},
	})

	if err != nil {
		log.Errorf("failed to generate original bucket access policy for %s: %s", bucket, err)
		return []byte{}, err
	}

	log.Debugf("generated policy with document %s", string(policyDoc))

	return policyDoc, nil
}

// temporaryAccessPolicy defines the IAM policy for temporary (user) access to upload data (RW)
func (s *S3Repository) temporaryAccessPolicy(bucket string) ([]byte, error) {
	log.Debugf("generating temporary bucket access policy for %s", bucket)

	policyDoc, err := json.Marshal(PolicyDoc{
		Version: "2012-10-17",
		Statement: []PolicyStatement{
			PolicyStatement{
				Resource: []string{fmt.Sprintf("arn:aws:s3:::%s", bucket)},
				Effect:   "Allow",
				Action: []string{
					"s3:ListBucket",
				},
			},
			PolicyStatement{
				Resource: []string{fmt.Sprintf("arn:aws:s3:::%s/*", bucket)},
				Effect:   "Allow",
				Action: []string{
					"s3:DeleteObject",
					"s3:GetObject",
					"s3:PutObject",
				},
			},
		},
	})

	if err != nil {
		log.Errorf("failed to generate temporary bucket access policy for %s: %s", bucket, err)
		return []byte{}, err
	}

	log.Debugf("generated policy with document %s", string(policyDoc))

	return policyDoc, nil
}
