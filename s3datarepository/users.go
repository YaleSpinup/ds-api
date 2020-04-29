package s3datarepository

import (
	"context"
	"fmt"

	"github.com/YaleSpinup/ds-api/apierror"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// ListUsers lists the users of a dataset with their key ids
func (s *S3Repository) ListUsers(ctx context.Context, id string) (map[string]interface{}, error) {
	if id == "" {
		return nil, apierror.New(apierror.ErrBadRequest, "invalid input", errors.New("empty id"))
	}

	name := id
	if s.NamePrefix != "" {
		name = s.NamePrefix + "-" + name
	}

	log.Infof("listing users of the s3datarepository %s", name)

	groupName := fmt.Sprintf("%s-DsTmpGrp", name)
	usersOutput, err := s.listGroupsUsers(ctx, groupName)
	if err != nil {
		return nil, err
	}

	log.Debugf("got iam users response %+v", usersOutput)

	output := make(map[string]interface{}, len(usersOutput))
	for _, u := range usersOutput {
		userName := aws.StringValue(u.UserName)
		keyOut, err := s.IAM.ListAccessKeysWithContext(ctx, &iam.ListAccessKeysInput{UserName: u.UserName})
		if err != nil {
			return nil, ErrCode("failed to getting access keys for user "+userName, err)
		}

		keys := make(map[string]string, len(keyOut.AccessKeyMetadata))
		for _, k := range keyOut.AccessKeyMetadata {
			keys[aws.StringValue(k.AccessKeyId)] = aws.StringValue(k.Status)
		}

		output[userName] = struct {
			Keys map[string]string `json:"keys"`
		}{keys}
	}

	log.Debugf("returning map of users for dataset %s: %+v", id, output)

	return output, nil
}

// listGroupsUsers lists the users that belong to a group
func (s *S3Repository) listGroupsUsers(ctx context.Context, groupName string) ([]*iam.User, error) {
	users := []*iam.User{}
	if groupName == "" {
		return users, apierror.New(apierror.ErrBadRequest, "invalid input", errors.New("empty group name"))
	}

	log.Infof("listing iam users for group %s", groupName)

	input := &iam.GetGroupInput{GroupName: aws.String(groupName)}

	truncated := true
	for truncated {
		output, err := s.IAM.GetGroupWithContext(ctx, input)
		if err != nil {
			return users, ErrCode("failed to getting users for group "+groupName, err)
		}

		truncated = aws.BoolValue(output.IsTruncated)
		users = append(users, output.Users...)
		input.Marker = output.Marker
	}

	return users, nil
}

// CreateUser creates a dataset user.
// - generates and creates the temporary access policy
// - create the temporary access group
// - attach the created policy to the created group
// - create the temporary user
// - create a set of access keys
// - add the user to the group
func (s *S3Repository) CreateUser(ctx context.Context, id string) (interface{}, error) {
	if id == "" {
		return nil, apierror.New(apierror.ErrBadRequest, "invalid input", errors.New("empty id"))
	}

	name := id
	if s.NamePrefix != "" {
		name = s.NamePrefix + "-" + name
	}

	log.Infof("creating user of the s3datarepository %s", name)

	policyDoc, err := s.temporaryAccessPolicy(name)
	if err != nil {
		return nil, ErrCode("generate temporary access policy for dataset "+id, err)
	}

	log.Debugf("generated temporary access policy document %s", string(policyDoc))

	// setup rollback function list and defer execution
	var rollBackTasks []func() error
	defer func() {
		if err != nil {
			log.Errorf("recovering from error creating user in s3datarepository: %s, executing %d rollback tasks", err, len(rollBackTasks))
			rollBack(&rollBackTasks)
		}
	}()

	policyOutput, err := s.IAM.CreatePolicyWithContext(ctx, &iam.CreatePolicyInput{
		Description:    aws.String("Temporary access policy for dataset " + id),
		Path:           aws.String(s.IAMPathPrefix),
		PolicyDocument: aws.String(string(policyDoc)),
		PolicyName:     aws.String(name + "-DsTmpPlc"),
	})
	if err != nil {
		return nil, ErrCode("create temporary access policy for dataset "+id, err)
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

	if err := s.IAM.WaitUntilPolicyExistsWithContext(ctx, &iam.GetPolicyInput{PolicyArn: policyOutput.Policy.Arn}); err != nil {
		return nil, ErrCode("waiting for temporary access policy to exist for dataset "+id, err)
	}

	log.Debugf("got iam create policy response %+v", policyOutput)

	groupName := name + "-DsTmpGrp"
	groupOutput, err := s.IAM.CreateGroupWithContext(ctx, &iam.CreateGroupInput{
		GroupName: aws.String(groupName),
		Path:      aws.String(s.IAMPathPrefix),
	})
	if err != nil {
		return nil, ErrCode("create group for dataset "+id, err)
	}

	// append group delete to rollback tasks
	rollBackTasks = append(rollBackTasks, func() error {
		return func() error {
			if _, err := s.IAM.DeleteGroupWithContext(ctx, &iam.DeleteGroupInput{GroupName: aws.String(groupName)}); err != nil {
				return err
			}
			return nil
		}()
	})

	log.Debugf("got iam create group response %+v", groupOutput)

	if _, err = s.IAM.AttachGroupPolicyWithContext(ctx, &iam.AttachGroupPolicyInput{
		GroupName: aws.String(groupName),
		PolicyArn: policyOutput.Policy.Arn,
	}); err != nil {
		return nil, ErrCode("attach policy to group for dataset "+id, err)
	}

	// append policy detach to rollback tasks
	rollBackTasks = append(rollBackTasks, func() error {
		return func() error {
			if _, err := s.IAM.DetachGroupPolicyWithContext(ctx, &iam.DetachGroupPolicyInput{
				GroupName: aws.String(groupName),
				PolicyArn: policyOutput.Policy.Arn,
			}); err != nil {
				return err
			}
			return nil
		}()
	})

	userName := name + "-DsTmpUsr"
	userOutput, err := s.IAM.CreateUserWithContext(ctx, &iam.CreateUserInput{
		Path:     aws.String(s.IAMPathPrefix),
		UserName: aws.String(userName),
	})
	if err != nil {
		return nil, ErrCode("create user for dataset "+id, err)
	}

	// append user delete to rollback tasks
	rollBackTasks = append(rollBackTasks, func() error {
		return func() error {
			if _, err := s.IAM.DeleteUserWithContext(ctx, &iam.DeleteUserInput{
				UserName: aws.String(userName),
			}); err != nil {
				return err
			}
			return nil
		}()
	})

	if err := s.IAM.WaitUntilUserExistsWithContext(ctx, &iam.GetUserInput{UserName: aws.String(userName)}); err != nil {
		return nil, ErrCode("waiting for user to exist for dataset "+id, err)
	}

	log.Debugf("got iam create user response %+v", userOutput)

	keyOutput, err := s.IAM.CreateAccessKeyWithContext(ctx, &iam.CreateAccessKeyInput{
		UserName: aws.String(userName),
	})
	if err != nil {
		return nil, ErrCode("create user access key for dataset "+id, err)
	}

	// append user delete to rollback tasks
	rollBackTasks = append(rollBackTasks, func() error {
		return func() error {
			if _, err := s.IAM.DeleteAccessKeyWithContext(ctx, &iam.DeleteAccessKeyInput{
				UserName:    aws.String(userName),
				AccessKeyId: keyOutput.AccessKey.AccessKeyId,
			}); err != nil {
				return err
			}
			return nil
		}()
	})

	if _, err = s.IAM.AddUserToGroupWithContext(ctx, &iam.AddUserToGroupInput{
		GroupName: aws.String(groupName),
		UserName:  aws.String(userName),
	}); err != nil {
		return nil, ErrCode("add user to group for dataset "+id, err)
	}

	log.Debugf("added user %s to group %s", userName, groupName)

	output := struct {
		Group       string            `json:"group"`
		Policy      string            `json:"policy"`
		User        string            `json:"user"`
		Credentials map[string]string `json:"credentials"`
	}{
		Group:  groupName,
		Policy: aws.StringValue(policyOutput.Policy.PolicyName),
		User:   userName,
		Credentials: map[string]string{
			"akid":   aws.StringValue(keyOutput.AccessKey.AccessKeyId),
			"secret": aws.StringValue(keyOutput.AccessKey.SecretAccessKey),
		},
	}

	return output, nil
}

// DeleteUser cleans up a dataset user.
//  - gets the group we manage
//  - detaches any policies from the group
//  - delete the policy we manage
//  - remove all of the users from the group
//  - deletes the credentials and the user we manage
//  - deletes the group
func (s *S3Repository) DeleteUser(ctx context.Context, id string) error {
	if id == "" {
		return apierror.New(apierror.ErrBadRequest, "invalid input", errors.New("empty id"))
	}

	name := id
	if s.NamePrefix != "" {
		name = s.NamePrefix + "-" + name
	}

	log.Infof("deleting user of the s3datarepository %s", name)

	groupName := name + "-DsTmpGrp"
	group, err := s.IAM.GetGroupWithContext(ctx, &iam.GetGroupInput{
		GroupName: aws.String(groupName),
	})
	if err != nil {
		return ErrCode("getting group for dataset "+id, err)
	}

	log.Debugf("found group '%s' %+v", groupName, group)

	groupPolicies, err := s.IAM.ListAttachedGroupPoliciesWithContext(ctx, &iam.ListAttachedGroupPoliciesInput{
		GroupName:  aws.String(groupName),
		PathPrefix: aws.String(s.IAMPathPrefix),
	})
	if err != nil {
		return ErrCode("listing attached group policies for dataset "+id, err)
	}

	log.Debugf("found attached group policies for '%s' %+v", groupName, groupPolicies)

	policyName := name + "-DsTmpPlc"
	for _, p := range groupPolicies.AttachedPolicies {
		log.Debugf("detaching policy %s from group %s", aws.StringValue(p.PolicyName), groupName)
		if _, err := s.IAM.DetachGroupPolicyWithContext(ctx, &iam.DetachGroupPolicyInput{
			GroupName: aws.String(groupName),
			PolicyArn: p.PolicyArn,
		}); err != nil {
			return ErrCode("detaching group policies for dataset "+id, err)
		}

		if aws.StringValue(p.PolicyName) == policyName {
			log.Debugf("deleting policy %s", policyName)
			if _, err := s.IAM.DeletePolicyWithContext(ctx, &iam.DeletePolicyInput{
				PolicyArn: p.PolicyArn,
			}); err != nil {
				return ErrCode("deleting policy for dataset "+id, err)
			}
		}
	}

	userName := name + "-DsTmpUsr"
	for _, u := range group.Users {
		log.Debugf("removing user %s from group %s", aws.StringValue(u.UserName), groupName)
		if _, err := s.IAM.RemoveUserFromGroupWithContext(ctx, &iam.RemoveUserFromGroupInput{
			GroupName: aws.String(groupName),
			UserName:  u.UserName,
		}); err != nil {
			return ErrCode("removing user from group for dataset "+id, err)
		}

		if aws.StringValue(u.UserName) == userName {
			keyOut, err := s.IAM.ListAccessKeysWithContext(ctx, &iam.ListAccessKeysInput{
				UserName: aws.String(userName),
			})
			if err != nil {
				return ErrCode("listing user access keys for dataset "+id, err)
			}

			for _, k := range keyOut.AccessKeyMetadata {
				log.Debugf("deleting user %s access key %s (%s)", userName, aws.StringValue(k.AccessKeyId), aws.StringValue(k.Status))
				if _, err := s.IAM.DeleteAccessKeyWithContext(ctx, &iam.DeleteAccessKeyInput{
					AccessKeyId: k.AccessKeyId,
					UserName:    aws.String(userName),
				}); err != nil {
					return ErrCode("deleting user access key for dataset "+id, err)
				}
			}

			log.Debugf("deleting user %s", userName)
			if _, err := s.IAM.DeleteUserWithContext(ctx, &iam.DeleteUserInput{
				UserName: aws.String(userName),
			}); err != nil {
				return ErrCode("deleting user for dataset "+id, err)
			}

		}
	}

	if _, err := s.IAM.DeleteGroupWithContext(ctx, &iam.DeleteGroupInput{
		GroupName: aws.String(groupName),
	}); err != nil {
		return ErrCode("deleting group for dataset "+id, err)
	}

	return nil
}

// UpdateUser manages the user keys.  This function should step through the lifecycle of a user's keys for a dataset...
// Provision key1 --> Provision key2, Make key1 Inactive --> Make key2 Inactive, Lock key generation.
//
// If there are no keys, one is created and made active. If there is one key, a new 'Active' key is generated. If
// there are 'Active keys all are made 'Inactive'. If there are two 'Inactive' keys, an error is returned to the caller.
// At any time, a user *should* only have one Active key.  Once the limit of two (2) keys is reached, manual intervention
// is required to regain access to the dataset via these credentials.
func (s *S3Repository) UpdateUser(ctx context.Context, id string) (map[string]interface{}, error) {
	if id == "" {
		return nil, apierror.New(apierror.ErrBadRequest, "invalid input", errors.New("empty id"))
	}

	name := id
	if s.NamePrefix != "" {
		name = s.NamePrefix + "-" + name
	}

	log.Infof("updating user of the s3datarepository %s", name)

	userName := name + "-DsTmpUsr"
	keysOut, err := s.IAM.ListAccessKeysWithContext(ctx, &iam.ListAccessKeysInput{
		UserName: aws.String(userName),
	})
	if err != nil {
		return nil, ErrCode("listing user access keys for dataset "+id, err)
	}

	log.Debugf("got user access keys output %+v", keysOut)

	// setup rollback function list and defer execution
	var rollBackTasks []func() error
	defer func() {
		if err != nil {
			log.Errorf("recovering from error updating user in s3datarepository: %s, executing %d rollback tasks", err, len(rollBackTasks))
			rollBack(&rollBackTasks)
		}
	}()

	output := make(map[string]interface{})
	if len(keysOut.AccessKeyMetadata) < 2 {
		keyOut, err := s.IAM.CreateAccessKeyWithContext(ctx, &iam.CreateAccessKeyInput{
			UserName: aws.String(userName),
		})
		if err != nil {
			return nil, ErrCode("create user access key for dataset "+id, err)
		}

		output["credentials"] = struct {
			KeyId  string `json:"akid"`
			Secret string `json:"secret"`
		}{
			aws.StringValue(keyOut.AccessKey.AccessKeyId),
			aws.StringValue(keyOut.AccessKey.SecretAccessKey),
		}

		// append user delete to rollback tasks
		rollBackTasks = append(rollBackTasks, func() error {
			return func() error {
				if _, err := s.IAM.DeleteAccessKeyWithContext(ctx, &iam.DeleteAccessKeyInput{
					UserName:    aws.String(userName),
					AccessKeyId: keyOut.AccessKey.AccessKeyId,
				}); err != nil {
					return err
				}
				return nil
			}()
		})
	}

	var inactive int
	keys := map[string]string{}
	for _, k := range keysOut.AccessKeyMetadata {
		if aws.StringValue(k.Status) == "Inactive" {
			inactive += 1
		}

		keys[aws.StringValue(k.AccessKeyId)] = aws.StringValue(k.Status)
		if aws.StringValue(k.Status) == "Active" {
			log.Debugf("deactivating access key %s for dataset %s", aws.StringValue(k.AccessKeyId), id)
			if _, err := s.IAM.UpdateAccessKeyWithContext(ctx, &iam.UpdateAccessKeyInput{
				AccessKeyId: k.AccessKeyId,
				Status:      aws.String("Inactive"),
				UserName:    aws.String(userName),
			}); err != nil {
				return nil, ErrCode("deactivating user access key for dataset "+id, err)
			}
			keys[aws.StringValue(k.AccessKeyId)] = "Inactive"
		}
	}
	output["keys"] = keys

	if inactive >= 2 {
		return nil, apierror.New(apierror.ErrLimitExceeded, "too many access keys (2)", nil)
	}

	return output, nil
}
