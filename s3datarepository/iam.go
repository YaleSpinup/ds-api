package s3datarepository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/YaleSpinup/ds-api/apierror"
	"github.com/YaleSpinup/ds-api/dataset"
	"github.com/aws/aws-sdk-go/aws"
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

// GrantAccess sets up the appropriate access to the data repository, depending if it's a derivative or not,
// and returns a list of Policy/Role names
func (s *S3Repository) GrantAccess(ctx context.Context, id string, derivative bool) (dataset.Access, error) {
	if id == "" {
		return nil, apierror.New(apierror.ErrBadRequest, "invalid input", errors.New("empty id"))
	}

	name := id
	if s.NamePrefix != "" {
		name = s.NamePrefix + "-" + name
	}

	log.Debugf("granting access to s3datarepository: %s (derivative: %t)", name, derivative)

	var policyDoc []byte
	var policyName string
	var err error

	if derivative {
		policyName = fmt.Sprintf("%s-DerivativePlc", name)
		policyDoc, err = s.derivativeAccessPolicy(name)
	} else {
		policyName = fmt.Sprintf("%s-OriginalPlc", name)
		policyDoc, err = s.originalAccessPolicy(name)
	}
	if err != nil {
		return nil, ErrCode("failed to generate IAM policy for bucket "+name, err)
	}

	log.Debugf("creating access policy for bucket '%s'", name)

	// setup rollback function list and defer execution
	var rollBackTasks []func() error
	defer func() {
		if err != nil {
			log.Errorf("recovering from error granting access to s3datarepository: %s, executing %d rollback tasks", err, len(rollBackTasks))
			rollBack(&rollBackTasks)
		}
	}()

	// create policy
	policyOutput, err := s.IAM.CreatePolicyWithContext(ctx, &iam.CreatePolicyInput{
		Description:    aws.String(fmt.Sprintf("Access policy for bucket %s", name)),
		Path:           aws.String(s.IAMPathPrefix),
		PolicyDocument: aws.String(string(policyDoc)),
		PolicyName:     aws.String(policyName),
	})
	if err != nil {
		return nil, ErrCode("failed to create IAM policy", err)
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

	// create role
	roleName := fmt.Sprintf("roleDataset_%s", id)
	roleDoc, err := s.assumeRolePolicy(name)
	if err != nil {
		return nil, ErrCode("failed to generate IAM assume role policy for bucket "+name, err)
	}

	log.Debugf("creating role for accessing bucket '%s'", name)

	roleOutput, err := s.IAM.CreateRoleWithContext(ctx, &iam.CreateRoleInput{
		AssumeRolePolicyDocument: aws.String(string(roleDoc)),
		Description:              aws.String(fmt.Sprintf("Role for accessing bucket %s", name)),
		Path:                     aws.String(s.IAMPathPrefix),
		RoleName:                 aws.String(roleName),
	})
	if err != nil {
		return nil, ErrCode("failed to create IAM role "+roleName, err)
	}

	// append role delete to rollback tasks
	rollBackTasks = append(rollBackTasks, func() error {
		return func() error {
			if _, err := s.IAM.DeleteRoleWithContext(ctx, &iam.DeleteRoleInput{RoleName: roleOutput.Role.RoleName}); err != nil {
				return err
			}
			return nil
		}()
	})

	// attach access policy to the role
	_, err = s.IAM.AttachRolePolicyWithContext(ctx, &iam.AttachRolePolicyInput{
		PolicyArn: policyOutput.Policy.Arn,
		RoleName:  aws.String(roleName),
	})
	if err != nil {
		return nil, ErrCode("failed to attach policy to role "+roleName, err)
	}

	// append policy detach from role to rollback tasks
	rollBackTasks = append(rollBackTasks, func() error {
		return func() error {
			if _, err := s.IAM.DetachRolePolicyWithContext(ctx, &iam.DetachRolePolicyInput{
				PolicyArn: policyOutput.Policy.Arn,
				RoleName:  aws.String(roleName),
			}); err != nil {
				return err
			}
			return nil
		}()
	})

	log.Debugf("created role %s", roleName)

	// create instance profile
	instanceProfileOutput, err := s.IAM.CreateInstanceProfileWithContext(ctx, &iam.CreateInstanceProfileInput{
		InstanceProfileName: aws.String(roleName),
		Path:                aws.String(s.IAMPathPrefix),
	})
	if err != nil {
		return nil, ErrCode("failed to create instance profile "+roleName, err)
	}

	// append instance profile delete to rollback tasks
	rollBackTasks = append(rollBackTasks, func() error {
		return func() error {
			if _, err := s.IAM.DeleteInstanceProfileWithContext(ctx, &iam.DeleteInstanceProfileInput{InstanceProfileName: aws.String(roleName)}); err != nil {
				return err
			}
			return nil
		}()
	})

	// add role to instance profile
	_, err = s.IAM.AddRoleToInstanceProfileWithContext(ctx, &iam.AddRoleToInstanceProfileInput{
		InstanceProfileName: aws.String(roleName),
		RoleName:            aws.String(roleName),
	})
	if err != nil {
		return nil, ErrCode("failed to add role to instance profile "+roleName, err)
	}

	// append role removal from instance profile to rollback tasks
	rollBackTasks = append(rollBackTasks, func() error {
		return func() error {
			if _, err := s.IAM.RemoveRoleFromInstanceProfileWithContext(ctx, &iam.RemoveRoleFromInstanceProfileInput{
				InstanceProfileName: aws.String(roleName),
				RoleName:            aws.String(roleName),
			}); err != nil {
				return err
			}
			return nil
		}()
	})

	log.Debugf("created instance profile %s", roleName)

	output := dataset.Access{
		"policy_arn":            aws.StringValue(policyOutput.Policy.Arn),
		"policy_name":           aws.StringValue(policyOutput.Policy.PolicyName),
		"role_arn":              aws.StringValue(roleOutput.Role.Arn),
		"role_name":             aws.StringValue(roleOutput.Role.RoleName),
		"instance_profile_arn":  aws.StringValue(instanceProfileOutput.InstanceProfile.Arn),
		"instance_profile_name": aws.StringValue(instanceProfileOutput.InstanceProfile.InstanceProfileName),
	}

	return output, nil
}

// RevokeAccess removes access to the data repository
func (s *S3Repository) RevokeAccess(ctx context.Context, id string) error {
	if id == "" {
		return apierror.New(apierror.ErrBadRequest, "invalid input", errors.New("empty id"))
	}

	name := id
	if s.NamePrefix != "" {
		name = s.NamePrefix + "-" + name
	}

	outputError := false
	roleName := fmt.Sprintf("roleDataset_%s", id)

	log.Infof("revoking access to s3datarepository: %s", name)

	// get list of policies attached to the IAM role for this repository
	policies, err := s.IAM.ListAttachedRolePoliciesWithContext(ctx, &iam.ListAttachedRolePoliciesInput{
		PathPrefix: aws.String(s.IAMPathPrefix),
		RoleName:   aws.String(roleName)})
	if err != nil {
		outputError = true
		log.Warnf("failed to list policies attached to role %s: %s", roleName, err)
	}

	// detach and delete all associated policies
	if policies != nil {
		for _, p := range policies.AttachedPolicies {
			if _, err = s.IAM.DetachRolePolicyWithContext(ctx, &iam.DetachRolePolicyInput{
				PolicyArn: p.PolicyArn,
				RoleName:  aws.String(roleName),
			}); err != nil {
				outputError = true
				log.Warnf("failed to detach policy %s: %s", aws.StringValue(p.PolicyArn), err)
			}

			if _, err = s.IAM.DeletePolicyWithContext(ctx, &iam.DeletePolicyInput{PolicyArn: p.PolicyArn}); err != nil {
				outputError = true
				log.Warnf("failed to delete policy %s: %s", aws.StringValue(p.PolicyArn), err)
			}
		}
	}

	// remove the role from the instance profile
	_, err = s.IAM.RemoveRoleFromInstanceProfileWithContext(ctx, &iam.RemoveRoleFromInstanceProfileInput{
		InstanceProfileName: aws.String(roleName),
		RoleName:            aws.String(roleName),
	})
	if err != nil {
		outputError = true
		log.Warnf("failed to remove role from instance profile %s: %s", roleName, err)
	}

	// delete the IAM instance profile (has the same name as the role)
	_, err = s.IAM.DeleteInstanceProfileWithContext(ctx, &iam.DeleteInstanceProfileInput{InstanceProfileName: aws.String(roleName)})
	if err != nil {
		outputError = true
		log.Warnf("failed to delete instance profile %s: %s", roleName, err)
	}

	// delete the IAM role
	_, err = s.IAM.DeleteRoleWithContext(ctx, &iam.DeleteRoleInput{RoleName: aws.String(roleName)})
	if err != nil {
		outputError = true
		log.Warnf("failed to delete role %s: %s", roleName, err)
	}

	if outputError {
		return apierror.New(apierror.ErrInternalError, "one or more errors trying to revoke access for data repository "+name, errors.New("revoke access failure"))
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
func (s *S3Repository) assumeRolePolicy(bucket string) ([]byte, error) {
	log.Debugf("generating assume role policy for %s", bucket)

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
		log.Errorf("failed to generate assume role policy for %s: %s", bucket, err)
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
