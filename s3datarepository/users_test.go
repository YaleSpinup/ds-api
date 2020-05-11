package s3datarepository

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/pkg/errors"
)

func (m *mockIAMClient) AddUserToGroupWithContext(ctx context.Context, input *iam.AddUserToGroupInput, opts ...request.Option) (*iam.AddUserToGroupOutput, error) {
	if err, ok := m.err["AddUserToGroupWithContext"]; ok && err != nil {
		return nil, err
	}

	m.t.Logf("AddUserToGroupWithContext: %+v", input)

	for _, tds := range testDatasets {
		if aws.StringValue(tds.group.group.GroupName) == aws.StringValue(input.GroupName) {
			for _, tu := range tds.users {
				if aws.StringValue(input.UserName) == aws.StringValue(tu.user.UserName) {
					return &iam.AddUserToGroupOutput{}, nil
				}
			}
		}
	}

	return nil, awserr.New(iam.ErrCodeNoSuchEntityException, "The group cannot be found.", nil)
}

func (m *mockIAMClient) AttachGroupPolicyWithContext(ctx context.Context, input *iam.AttachGroupPolicyInput, opts ...request.Option) (*iam.AttachGroupPolicyOutput, error) {
	if err, ok := m.err["AttachGroupPolicyWithContext"]; ok && err != nil {
		return nil, err
	}

	m.t.Logf("AttachGroupPolicyWithContext: %+v", input)

	for _, tds := range testDatasets {
		if tds.group == nil || tds.group.group == nil {
			continue
		}

		if aws.StringValue(input.GroupName) == aws.StringValue(tds.group.group.GroupName) {
			pArn := fmt.Sprintf("arn:aws:iam::12345678901:policy/test/dataset-%s-DsTmpPlc", tds.id)
			if aws.StringValue(input.PolicyArn) != pArn {
				return nil, awserr.New(iam.ErrCodeNoSuchEntityException, " The iam policy cannot be found.", nil)
			}
			return &iam.AttachGroupPolicyOutput{}, nil
		}
	}

	return nil, awserr.New(iam.ErrCodeNoSuchEntityException, "The group cannot be found.", nil)
}

func (m *mockIAMClient) CreateAccessKeyWithContext(ctx context.Context, input *iam.CreateAccessKeyInput, opts ...request.Option) (*iam.CreateAccessKeyOutput, error) {
	if err, ok := m.err["CreateAccessKeyWithContext"]; ok && err != nil {
		return nil, err
	}

	m.t.Logf("CreateAccessKeyWithContext: %+v", input)

	for _, tds := range testDatasets {
		for _, u := range tds.users {
			if aws.StringValue(input.UserName) == aws.StringValue(u.user.UserName) {
				return &iam.CreateAccessKeyOutput{
					AccessKey: &iam.AccessKey{
						AccessKeyId:     aws.String(aws.StringValue(input.UserName) + "KEY"),
						SecretAccessKey: aws.String(aws.StringValue(input.UserName) + "SECRET"),
					},
				}, nil
			}
		}
	}

	return nil, awserr.New(iam.ErrCodeNoSuchEntityException, "The user cannot be found.", nil)
}

func (m *mockIAMClient) CreateGroupWithContext(ctx context.Context, input *iam.CreateGroupInput, opts ...request.Option) (*iam.CreateGroupOutput, error) {
	if err, ok := m.err["CreateGroupWithContext"]; ok && err != nil {
		return nil, err
	}

	m.t.Logf("CreateGroupWithContext: %+v", input)

	for _, tds := range testDatasets {
		m.t.Logf("testing tds: %+v", tds)

		if tds.group == nil || tds.group.group == nil {
			continue
		}

		if aws.StringValue(input.GroupName) == aws.StringValue(tds.group.group.GroupName) {
			return &iam.CreateGroupOutput{
				Group: tds.group.group,
			}, nil
		}
	}

	return nil, awserr.New(iam.ErrCodeNoSuchEntityException, "The group cannot be found.", nil)
}

func (m *mockIAMClient) CreateUserWithContext(ctx context.Context, input *iam.CreateUserInput, opts ...request.Option) (*iam.CreateUserOutput, error) {
	if err, ok := m.err["CreateUserWithContext"]; ok && err != nil {
		return nil, err
	}

	m.t.Logf("CreateUserWithContext: %+v", input)

	for _, tds := range testDatasets {
		for _, u := range tds.users {
			if aws.StringValue(input.UserName) == aws.StringValue(u.user.UserName) {
				return &iam.CreateUserOutput{
					User: u.user,
				}, nil
			}
		}
	}

	return nil, awserr.New(iam.ErrCodeNoSuchEntityException, "The user cannot be found.", nil)
}

func (m *mockIAMClient) DeleteAccessKeyWithContext(ctx context.Context, input *iam.DeleteAccessKeyInput, opts ...request.Option) (*iam.DeleteAccessKeyOutput, error) {
	if err, ok := m.err["DeleteAccessKeyWithContext"]; ok && err != nil {
		return nil, err
	}

	m.t.Logf("DeleteAccessKeyWithContext: %+v", input)

	for _, tds := range testDatasets {
		for _, u := range tds.users {
			if aws.StringValue(input.UserName) == aws.StringValue(u.user.UserName) {
				for _, k := range u.accessKeys {
					if aws.StringValue(k.AccessKeyId) == aws.StringValue(input.AccessKeyId) {
						return &iam.DeleteAccessKeyOutput{}, nil
					}
				}
			}
		}
	}

	return nil, awserr.New(iam.ErrCodeNoSuchEntityException, "The user cannot be found.", nil)
}

func (m *mockIAMClient) DeleteGroupWithContext(ctx context.Context, input *iam.DeleteGroupInput, opts ...request.Option) (*iam.DeleteGroupOutput, error) {
	if err, ok := m.err["DeleteGroupWithContext"]; ok && err != nil {
		return nil, err
	}

	m.t.Logf("DeleteGroupWithContext: %+v", input)

	for _, tds := range testDatasets {
		if tds.group == nil || tds.group.group == nil {
			continue
		}

		if aws.StringValue(tds.group.group.GroupName) == aws.StringValue(input.GroupName) {
			return &iam.DeleteGroupOutput{}, nil
		}
	}

	return nil, awserr.New("Forbidden", "No access to group", nil)
}

func (m *mockIAMClient) DeleteUserWithContext(ctx context.Context, input *iam.DeleteUserInput, opts ...request.Option) (*iam.DeleteUserOutput, error) {
	if err, ok := m.err["DeleteUserWithContext"]; ok && err != nil {
		return nil, err
	}

	m.t.Logf("DeleteUserWithContext: %+v", input)

	for _, tds := range testDatasets {
		for _, u := range tds.users {
			if aws.StringValue(input.UserName) == aws.StringValue(u.user.UserName) {
				return &iam.DeleteUserOutput{}, nil
			}
		}
	}

	return nil, awserr.New(iam.ErrCodeNoSuchEntityException, "The user cannot be found.", nil)
}

func (m *mockIAMClient) DetachGroupPolicyWithContext(ctx context.Context, input *iam.DetachGroupPolicyInput, opts ...request.Option) (*iam.DetachGroupPolicyOutput, error) {
	if err, ok := m.err["DetachGroupPolicyWithContext"]; ok && err != nil {
		return nil, err
	}

	m.t.Logf("DetachGroupPolicyWithContext: %+v", input)

	for _, tds := range testDatasets {
		if tds.group == nil || tds.group.group == nil {
			continue
		}

		if aws.StringValue(input.GroupName) == aws.StringValue(tds.group.group.GroupName) {
			pArn := fmt.Sprintf("arn:aws:iam::12345678901:policy/test/dataset-%s-DsTmpPlc", tds.id)
			if aws.StringValue(input.PolicyArn) != pArn {
				return nil, awserr.New(iam.ErrCodeNoSuchEntityException, "The iam policy cannot be found.", nil)
			}
			return &iam.DetachGroupPolicyOutput{}, nil
		}
	}

	return nil, awserr.New(iam.ErrCodeNoSuchEntityException, "The group cannot be found.", nil)

}

func (m *mockIAMClient) GetGroupWithContext(ctx context.Context, input *iam.GetGroupInput, opts ...request.Option) (*iam.GetGroupOutput, error) {
	if err, ok := m.err["GetGroupWithContext"]; ok && err != nil {
		return nil, err
	}

	m.t.Logf("GetGroupWithContext: %+v", input)

	for _, tds := range testDatasets {
		if tds.group == nil || tds.group.group == nil {
			continue
		}

		if aws.StringValue(tds.group.group.GroupName) == aws.StringValue(input.GroupName) {
			output := &iam.GetGroupOutput{
				Group: tds.group.group,
				Users: []*iam.User{},
			}

			for _, u := range tds.users {
				output.Users = append(output.Users, u.user)
			}

			return output, nil
		}
	}

	return nil, awserr.New("Forbidden", "No access to group", nil)
}

func (m *mockIAMClient) ListAccessKeysWithContext(ctx context.Context, input *iam.ListAccessKeysInput, opts ...request.Option) (*iam.ListAccessKeysOutput, error) {
	if err, ok := m.err["ListAccessKeysWithContext"]; ok && err != nil {
		return nil, err
	}

	m.t.Logf("ListAccessKeysWithContext: %+v", input)

	output := &iam.ListAccessKeysOutput{}
	for _, tds := range testDatasets {
		for _, u := range tds.users {
			if aws.StringValue(input.UserName) == aws.StringValue(u.user.UserName) {
				output.AccessKeyMetadata = u.accessKeys
			}
		}
	}

	return output, nil
}

func (m *mockIAMClient) ListAttachedGroupPoliciesWithContext(ctx context.Context, input *iam.ListAttachedGroupPoliciesInput, opts ...request.Option) (*iam.ListAttachedGroupPoliciesOutput, error) {
	if err, ok := m.err["ListAttachedGroupPoliciesWithContext"]; ok && err != nil {
		return nil, err
	}

	m.t.Logf("ListAttachedGroupPoliciesWithContext: %+v", input)

	for _, tds := range testDatasets {
		if aws.StringValue(input.GroupName) == aws.StringValue(tds.group.group.GroupName) {
			return &iam.ListAttachedGroupPoliciesOutput{
				AttachedPolicies: []*iam.AttachedPolicy{
					{
						PolicyArn:  aws.String(fmt.Sprintf("arn:aws:iam::12345678901:policy/test/dataset-%s-DsTmpPlc", tds.id)),
						PolicyName: aws.String(fmt.Sprintf("dataset-%s-DsTmpPlc", tds.id)),
					},
				},
			}, nil
		}
	}

	return nil, awserr.New(iam.ErrCodeNoSuchEntityException, "The group cannot be found.", nil)
}

func (m *mockIAMClient) RemoveUserFromGroupWithContext(ctx context.Context, input *iam.RemoveUserFromGroupInput, opts ...request.Option) (*iam.RemoveUserFromGroupOutput, error) {
	if err, ok := m.err["RemoveUserFromGroupWithContext"]; ok && err != nil {
		return nil, err
	}

	m.t.Logf("RemoveUserFromGroupWithContext: %+v", input)

	for _, tds := range testDatasets {
		if aws.StringValue(tds.group.group.GroupName) == aws.StringValue(input.GroupName) {
			for _, tu := range tds.users {
				if aws.StringValue(input.UserName) == aws.StringValue(tu.user.UserName) {
					return &iam.RemoveUserFromGroupOutput{}, nil
				}
			}
		}
	}

	return nil, awserr.New(iam.ErrCodeNoSuchEntityException, "The group cannot be found.", nil)
}

func (m *mockIAMClient) UpdateAccessKeyWithContext(ctx context.Context, input *iam.UpdateAccessKeyInput, opts ...request.Option) (*iam.UpdateAccessKeyOutput, error) {
	if err, ok := m.err["UpdateAccessKeyWithContext"]; ok && err != nil {
		return nil, err
	}

	m.t.Logf("UpdateAccessKeyWithContext: %+v", input)

	for _, tds := range testDatasets {
		for _, u := range tds.users {
			if aws.StringValue(input.UserName) == aws.StringValue(u.user.UserName) {
				for _, k := range u.accessKeys {
					if aws.StringValue(k.AccessKeyId) == aws.StringValue(input.AccessKeyId) {
						k.Status = input.Status
						return &iam.UpdateAccessKeyOutput{}, nil
					}
				}
			}
		}
	}

	return nil, awserr.New(iam.ErrCodeNoSuchEntityException, "The user cannot be found.", nil)
}

func (m *mockIAMClient) WaitUntilPolicyExistsWithContext(ctx context.Context, input *iam.GetPolicyInput, opts ...request.WaiterOption) error {
	if err, ok := m.err["WaitUntilPolicyExistsWithContext"]; ok && err != nil {
		return err
	}
	return nil
}

func (m *mockIAMClient) WaitUntilUserExistsWithContext(ctx context.Context, input *iam.GetUserInput, opts ...request.WaiterOption) error {
	if err, ok := m.err["WaitUntilUserExistsWithContext"]; ok && err != nil {
		return err
	}
	return nil
}

type testUser struct {
	user       *iam.User
	accessKeys []*iam.AccessKeyMetadata
}

type testGroup struct {
	group *iam.Group
}

type testDataset struct {
	id    string
	group *testGroup
	users []*testUser
	merr  map[string]error
	err   error
}

var testDatasets []testDataset

func newTestDatasets() []testDataset {
	testDatasets = nil
	return []testDataset{
		{
			id: "a44ef4bf-01ce-4073-a738-fa33792624ae",
			group: &testGroup{
				group: &iam.Group{
					GroupName: aws.String("dataset-a44ef4bf-01ce-4073-a738-fa33792624ae-DsTmpGrp"),
				},
			},
			users: []*testUser{
				{
					user: &iam.User{
						Arn:      aws.String("arn:aws:iam::12345678901:users/dataset-a44ef4bf-01ce-4073-a738-fa33792624ae-DsTmpUsr"),
						UserName: aws.String("dataset-a44ef4bf-01ce-4073-a738-fa33792624ae-DsTmpUsr"),
					},
					accessKeys: []*iam.AccessKeyMetadata{},
				},
			},
		},
		{
			id: "b6977427-366f-46b7-881a-02452bf0110d",
			group: &testGroup{
				group: &iam.Group{
					GroupName: aws.String("dataset-b6977427-366f-46b7-881a-02452bf0110d-DsTmpGrp"),
				},
			},
			users: []*testUser{
				{
					user: &iam.User{
						Arn:      aws.String("arn:aws:iam::12345678901:users/dataset-b6977427-366f-46b7-881a-02452bf0110d-DsTmpUsr"),
						UserName: aws.String("dataset-b6977427-366f-46b7-881a-02452bf0110d-DsTmpUsr"),
					},
					accessKeys: []*iam.AccessKeyMetadata{
						{
							AccessKeyId: aws.String("ABCDEFG"),
							Status:      aws.String("Active"),
						},
					},
				},
			},
		},
		{
			id: "a485eb25-8346-4bd2-a540-867dff61aa7b",
			group: &testGroup{
				group: &iam.Group{
					GroupName: aws.String("dataset-a485eb25-8346-4bd2-a540-867dff61aa7b-DsTmpGrp"),
				},
			},
			users: []*testUser{
				{
					user: &iam.User{
						Arn:      aws.String("arn:aws:iam::12345678901:users/dataset-a485eb25-8346-4bd2-a540-867dff61aa7b-DsTmpUsr"),
						UserName: aws.String("dataset-a485eb25-8346-4bd2-a540-867dff61aa7b-DsTmpUsr"),
					},
					accessKeys: []*iam.AccessKeyMetadata{
						{
							AccessKeyId: aws.String("HIJKLMN"),
							Status:      aws.String("Inactive"),
						},
					},
				},
			},
		},
		{
			id: "9c9f2dbd-100c-4f8e-b4ad-03a399cd745a",
			group: &testGroup{
				group: &iam.Group{
					GroupName: aws.String("dataset-9c9f2dbd-100c-4f8e-b4ad-03a399cd745a-DsTmpGrp"),
				},
			},
			users: []*testUser{
				{
					user: &iam.User{
						Arn:      aws.String("arn:aws:iam::12345678901:users/dataset-9c9f2dbd-100c-4f8e-b4ad-03a399cd745a-DsTmpUsr"),
						UserName: aws.String("dataset-9c9f2dbd-100c-4f8e-b4ad-03a399cd745a-DsTmpUsr"),
					},
					accessKeys: []*iam.AccessKeyMetadata{
						{
							AccessKeyId: aws.String("OPQRSTUV"),
							Status:      aws.String("Inactive"),
						},
						{
							AccessKeyId: aws.String("WXYZ12"),
							Status:      aws.String("Active"),
						},
					},
				},
			},
		},
		{
			id: "c1fdc576-0467-474c-acb8-f6e35f75b8d3",
			group: &testGroup{
				group: &iam.Group{
					GroupName: aws.String("dataset-c1fdc576-0467-474c-acb8-f6e35f75b8d3-DsTmpGrp"),
				},
			},
			users: []*testUser{
				{
					user: &iam.User{
						Arn:      aws.String("arn:aws:iam::12345678901:users/dataset-c1fdc576-0467-474c-acb8-f6e35f75b8d3-DsTmpUsr"),
						UserName: aws.String("dataset-c1fdc576-0467-474c-acb8-f6e35f75b8d3-DsTmpUsr"),
					},
					accessKeys: []*iam.AccessKeyMetadata{
						{
							AccessKeyId: aws.String("3456789"),
							Status:      aws.String("Inactive"),
						},
						{
							AccessKeyId: aws.String("10111213"),
							Status:      aws.String("Inactive"),
						},
					},
				},
			},
		},
	}
}

func TestListUsers(t *testing.T) {
	listUserErrTests := []testDataset{
		{
			id:  "",
			err: errors.New("BadRequest: invalid input (empty id)"),
		},
		{
			id: "6639faf1-b9a2-4882-b4ff-36b429a80c6f",
			group: &testGroup{
				group: &iam.Group{
					GroupName: aws.String("dataset-6639faf1-b9a2-4882-b4ff-36b429a80c6f-DsTmpGrp"),
				},
			},
			users: []*testUser{
				{
					user: &iam.User{
						Arn:      aws.String("arn:aws:iam::12345678901:users/dataset-6639faf1-b9a2-4882-b4ff-36b429a80c6f-DsTmpUsr"),
						UserName: aws.String("dataset-6639faf1-b9a2-4882-b4ff-36b429a80c6f-DsTmpUsr"),
					},
					accessKeys: []*iam.AccessKeyMetadata{},
				},
			},
			err: errors.New("InternalError: failed getting access keys for user dataset-6639faf1-b9a2-4882-b4ff-36b429a80c6f-DsTmpUsr (TestListUsers ListAccessKeysWithContext)"),
			merr: map[string]error{
				"ListAccessKeysWithContext": errors.New("TestListUsers ListAccessKeysWithContext"),
			},
		},
	}

	testDatasets = append(newTestDatasets(), listUserErrTests...)

	t.Logf("length of tests: %d", len(testDatasets))

	for _, tds := range testDatasets {
		s := newTestS3Repository(t)
		if tds.merr != nil {
			s.IAM.(*mockIAMClient).err = tds.merr
			if _, err := s.ListUsers(context.TODO(), tds.id); err != nil {
				if err.Error() != tds.err.Error() {
					t.Errorf("expected error '%s', got '%s'", tds.err, err)
				}
			} else {
				t.Error("expected error, got nil")
			}
		}

		expected := make(map[string]interface{})
		for _, u := range tds.users {
			name := aws.StringValue(u.user.UserName)
			keys := make(map[string]string)
			for _, k := range u.accessKeys {
				keys[aws.StringValue(k.AccessKeyId)] = aws.StringValue(k.Status)
			}

			expected[name] = struct {
				Keys map[string]string `json:"keys"`
			}{keys}
		}

		out, err := s.ListUsers(context.TODO(), tds.id)
		if err != nil && tds.err == nil {
			t.Errorf("expected nil error, got %s", err)
		} else if err == nil && tds.err != nil {
			t.Errorf("expected err %s, got nil", tds.err)
		} else if err != nil {
			if tds.err.Error() != err.Error() {
				t.Errorf("expected error %s, got %s", tds.err, err)
			}
		} else {
			t.Logf("got output %+v", out)

			if !reflect.DeepEqual(expected, out) {
				t.Errorf("expected %+v, got %+v", expected, out)
			}
		}
	}
}

func TestListGroupUsers(t *testing.T) {
	listGroupUserErrTests := []testDataset{
		{
			id: "d77a6341-8faa-4871-9c79-5f53e634045c",
			group: &testGroup{
				group: &iam.Group{
					GroupName: aws.String(""),
				},
			},
			err: errors.New("BadRequest: invalid input (empty group name)"),
		},
		{
			id: "1d210026-3696-4268-89e6-f20cfe7128ad",
			group: &testGroup{
				group: &iam.Group{
					GroupName: aws.String("dataset-1d210026-3696-4268-89e6-f20cfe7128ad-DsTmpGrp"),
				},
			},
			users: []*testUser{
				{
					user: &iam.User{
						Arn:      aws.String("arn:aws:iam::12345678901:users/dataset-61d210026-3696-4268-89e6-f20cfe7128ad-DsTmpUsr"),
						UserName: aws.String("dataset-1d210026-3696-4268-89e6-f20cfe7128ad-DsTmpUsr"),
					},
					accessKeys: []*iam.AccessKeyMetadata{},
				},
			},
			err: errors.New("InternalError: failed getting users for group dataset-1d210026-3696-4268-89e6-f20cfe7128ad-DsTmpGrp (TestListGroupUsers GetGroupWithContext)"),
			merr: map[string]error{
				"GetGroupWithContext": errors.New("TestListGroupUsers GetGroupWithContext"),
			},
		},
	}
	testDatasets = append(newTestDatasets(), listGroupUserErrTests...)

	t.Logf("length of tests: %d", len(testDatasets))

	for _, tds := range testDatasets {
		s := newTestS3Repository(t)
		if tds.merr != nil {
			s.IAM.(*mockIAMClient).err = tds.merr
			if _, err := s.listGroupsUsers(context.TODO(), aws.StringValue(tds.group.group.GroupName)); err != nil {
				if err.Error() != tds.err.Error() {
					t.Errorf("expected error '%s', got '%s'", tds.err, err)
				}
			} else {
				t.Error("expected error, got nil")
			}
		}

		expected := []*iam.User{}
		for _, u := range tds.users {
			expected = append(expected, u.user)
		}

		out, err := s.listGroupsUsers(context.TODO(), aws.StringValue(tds.group.group.GroupName))
		if err != nil && tds.err == nil {
			t.Errorf("expected nil error, got %s", err)
		} else if err == nil && tds.err != nil {
			t.Errorf("expected err %s, got nil", tds.err)
		} else if err != nil {
			if tds.err.Error() != err.Error() {
				t.Errorf("expected error %s, got %s", tds.err, err)
			}
		} else {
			t.Logf("got output %+v", out)

			if !reflect.DeepEqual(expected, out) {
				t.Errorf("expected %+v, got %+v", expected, out)
			}
		}
	}
}

func TestCreateUser(t *testing.T) {
	createUserErrTests := []testDataset{
		{
			id:  "",
			err: errors.New("BadRequest: invalid input (empty id)"),
		},
		{
			id: "47db569b-06fc-4679-8a17-57fe2506d6d7",
			group: &testGroup{
				group: &iam.Group{
					GroupName: aws.String("dataset-47db569b-06fc-4679-8a17-57fe2506d6d7-DsTmpGrp"),
				},
			},
			users: []*testUser{
				{
					user: &iam.User{
						Arn:      aws.String("arn:aws:iam::12345678901:users/dataset-47db569b-06fc-4679-8a17-57fe2506d6d7-DsTmpUsr"),
						UserName: aws.String("dataset-47db569b-06fc-4679-8a17-57fe2506d6d7-DsTmpUsr"),
					},
					accessKeys: []*iam.AccessKeyMetadata{},
				},
			},
			err: errors.New("InternalError: waiting for temporary access policy to exist for dataset 47db569b-06fc-4679-8a17-57fe2506d6d7 (TestCreateUser WaitUntilPolicyExistsWithContext)"),
			merr: map[string]error{
				"WaitUntilPolicyExistsWithContext": errors.New("TestCreateUser WaitUntilPolicyExistsWithContext"),
			},
		},
		{
			id: "3fad3da9-b485-4e19-a71d-77bb638964ed",
			group: &testGroup{
				group: &iam.Group{
					GroupName: aws.String("dataset-3fad3da9-b485-4e19-a71d-77bb638964ed-DsTmpGrp"),
				},
			},
			users: []*testUser{
				{
					user: &iam.User{
						Arn:      aws.String("arn:aws:iam::12345678901:users/dataset-3fad3da9-b485-4e19-a71d-77bb638964ed-DsTmpUsr"),
						UserName: aws.String("dataset-3fad3da9-b485-4e19-a71d-77bb638964ed-DsTmpUsr"),
					},
					accessKeys: []*iam.AccessKeyMetadata{},
				},
			},
			err: errors.New("InternalError: create group for dataset 3fad3da9-b485-4e19-a71d-77bb638964ed (TestCreateUser CreateGroupWithContext)"),
			merr: map[string]error{
				"CreateGroupWithContext": errors.New("TestCreateUser CreateGroupWithContext"),
			},
		},
		{
			id: "8462a15d-5456-4ec2-98ee-c6a3a9a055e9",
			group: &testGroup{
				group: &iam.Group{
					GroupName: aws.String("dataset-8462a15d-5456-4ec2-98ee-c6a3a9a055e9-DsTmpGrp"),
				},
			},
			users: []*testUser{
				{
					user: &iam.User{
						Arn:      aws.String("arn:aws:iam::12345678901:users/dataset-8462a15d-5456-4ec2-98ee-c6a3a9a055e9-DsTmpUsr"),
						UserName: aws.String("dataset-8462a15d-5456-4ec2-98ee-c6a3a9a055e9-DsTmpUsr"),
					},
					accessKeys: []*iam.AccessKeyMetadata{},
				},
			},
			err: errors.New("InternalError: attach policy to group for dataset 8462a15d-5456-4ec2-98ee-c6a3a9a055e9 (TestCreateUser AttachGroupPolicyWithContext)"),
			merr: map[string]error{
				"AttachGroupPolicyWithContext": errors.New("TestCreateUser AttachGroupPolicyWithContext"),
			},
		},
		{
			id: "5a1a1e09-a837-4b4e-84fd-0fb47d14b7fe",
			group: &testGroup{
				group: &iam.Group{
					GroupName: aws.String("dataset-5a1a1e09-a837-4b4e-84fd-0fb47d14b7fe-DsTmpGrp"),
				},
			},
			users: []*testUser{
				{
					user: &iam.User{
						Arn:      aws.String("arn:aws:iam::12345678901:users/dataset-5a1a1e09-a837-4b4e-84fd-0fb47d14b7fe-DsTmpUsr"),
						UserName: aws.String("dataset-5a1a1e09-a837-4b4e-84fd-0fb47d14b7fe-DsTmpUsr"),
					},
					accessKeys: []*iam.AccessKeyMetadata{},
				},
			},
			err: errors.New("InternalError: create user for dataset 5a1a1e09-a837-4b4e-84fd-0fb47d14b7fe (TestCreateUser CreateUserWithContext)"),
			merr: map[string]error{
				"CreateUserWithContext": errors.New("TestCreateUser CreateUserWithContext"),
			},
		},
		{
			id: "8447af60-1174-49a0-bc06-3ea18755fa25",
			group: &testGroup{
				group: &iam.Group{
					GroupName: aws.String("dataset-8447af60-1174-49a0-bc06-3ea18755fa25-DsTmpGrp"),
				},
			},
			users: []*testUser{
				{
					user: &iam.User{
						Arn:      aws.String("arn:aws:iam::12345678901:users/dataset-8447af60-1174-49a0-bc06-3ea18755fa25-DsTmpUsr"),
						UserName: aws.String("dataset-8447af60-1174-49a0-bc06-3ea18755fa25-DsTmpUsr"),
					},
					accessKeys: []*iam.AccessKeyMetadata{},
				},
			},
			err: errors.New("InternalError: waiting for user to exist for dataset 8447af60-1174-49a0-bc06-3ea18755fa25 (TestCreateUser WaitUntilUserExistsWithContext)"),
			merr: map[string]error{
				"WaitUntilUserExistsWithContext": errors.New("TestCreateUser WaitUntilUserExistsWithContext"),
			},
		},
		{
			id: "0d987a82-fa02-420b-8ea0-46d4d63a86a6",
			group: &testGroup{
				group: &iam.Group{
					GroupName: aws.String("dataset-0d987a82-fa02-420b-8ea0-46d4d63a86a6-DsTmpGrp"),
				},
			},
			users: []*testUser{
				{
					user: &iam.User{
						Arn:      aws.String("arn:aws:iam::12345678901:users/dataset-0d987a82-fa02-420b-8ea0-46d4d63a86a6-DsTmpUsr"),
						UserName: aws.String("dataset-0d987a82-fa02-420b-8ea0-46d4d63a86a6-DsTmpUsr"),
					},
					accessKeys: []*iam.AccessKeyMetadata{},
				},
			},
			err: errors.New("InternalError: create user access key for dataset 0d987a82-fa02-420b-8ea0-46d4d63a86a6 (TestCreateUser CreateAccessKeyWithContext)"),
			merr: map[string]error{
				"CreateAccessKeyWithContext": errors.New("TestCreateUser CreateAccessKeyWithContext"),
			},
		},
		{
			id: "3830f909-bfd7-4bae-87bd-cf9f0aeeac20",
			group: &testGroup{
				group: &iam.Group{
					GroupName: aws.String("dataset-3830f909-bfd7-4bae-87bd-cf9f0aeeac20-DsTmpGrp"),
				},
			},
			users: []*testUser{
				{
					user: &iam.User{
						Arn:      aws.String("arn:aws:iam::12345678901:users/dataset-3830f909-bfd7-4bae-87bd-cf9f0aeeac20-DsTmpUsr"),
						UserName: aws.String("dataset-3830f909-bfd7-4bae-87bd-cf9f0aeeac20-DsTmpUsr"),
					},
					accessKeys: []*iam.AccessKeyMetadata{},
				},
			},
			err: errors.New("InternalError: add user to group for dataset 3830f909-bfd7-4bae-87bd-cf9f0aeeac20 (TestCreateUser AddUserToGroupWithContext)"),
			merr: map[string]error{
				"AddUserToGroupWithContext": errors.New("TestCreateUser AddUserToGroupWithContext"),
			},
		},
	}
	testDatasets = append(newTestDatasets(), createUserErrTests...)

	t.Logf("length of tests: %d", len(testDatasets))

	for _, tds := range testDatasets {
		s := newTestS3Repository(t)
		if tds.merr != nil {
			s.IAM.(*mockIAMClient).err = tds.merr
			if _, err := s.CreateUser(context.TODO(), tds.id); err != nil {
				if err.Error() != tds.err.Error() {
					t.Errorf("expected error '%s', got '%s'", tds.err, err)
				}
			} else {
				t.Error("expected error, got nil")
			}
			continue
		}

		t.Logf("testing with tds: %+v\n", tds)

		expected := struct {
			Group       string            `json:"group"`
			Policy      string            `json:"policy"`
			User        string            `json:"user"`
			Credentials map[string]string `json:"credentials"`
		}{
			Group:  fmt.Sprintf("dataset-%s-DsTmpGrp", tds.id),
			Policy: fmt.Sprintf("dataset-%s-DsTmpPlc", tds.id),
			User:   fmt.Sprintf("dataset-%s-DsTmpUsr", tds.id),
			Credentials: map[string]string{
				"akid":   fmt.Sprintf("dataset-%s-DsTmpUsrKEY", tds.id),
				"secret": fmt.Sprintf("dataset-%s-DsTmpUsrSECRET", tds.id),
			},
		}

		out, err := s.CreateUser(context.TODO(), tds.id)
		if err != nil && tds.err == nil {
			t.Errorf("expected nil error, got %s", err)
		} else if err == nil && tds.err != nil {
			t.Errorf("expected err %s, got nil", tds.err)
		} else if err != nil {
			if tds.err.Error() != err.Error() {
				t.Errorf("expected error %s, got %s", tds.err, err)
			}
		} else {
			if !reflect.DeepEqual(expected, out) {
				t.Errorf("expected %+v, got %+v", expected, out)
			}
		}
	}
}

func TestDeleteUser(t *testing.T) {
	deleteUserErrTests := []testDataset{
		{
			id:  "",
			err: errors.New("BadRequest: invalid input (empty id)"),
		},
	}
	testDatasets = append(newTestDatasets(), deleteUserErrTests...)

	t.Logf("length of tests: %d", len(testDatasets))

	for _, tds := range testDatasets {
		s := newTestS3Repository(t)
		if tds.merr != nil {
			s.IAM.(*mockIAMClient).err = tds.merr
			if err := s.DeleteUser(context.TODO(), tds.id); err != nil {
				if err.Error() != tds.err.Error() {
					t.Errorf("expected error '%s', got '%s'", tds.err, err)
				}
			} else {
				t.Error("expected error, got nil")
			}
			continue
		}

		t.Logf("testing with tds: %+v\n", tds)

		if err := s.DeleteUser(context.TODO(), tds.id); err != nil && tds.err == nil {
			t.Errorf("expected nil error, got %s", err)
		} else if err == nil && tds.err != nil {
			t.Errorf("expected err %s, got nil", tds.err)
		} else if err != nil {
			if tds.err.Error() != err.Error() {
				t.Errorf("expected error %s, got %s", tds.err, err)
			}
		}
	}
}

func TestUpdateUser(t *testing.T) {
	updateUserErrTests := []testDataset{
		{
			id:  "",
			err: errors.New("BadRequest: invalid input (empty id)"),
		},
		{
			id: "091faadd-b8b8-449c-a6ba-88f9c5020556",
			group: &testGroup{
				group: &iam.Group{
					GroupName: aws.String("dataset-091faadd-b8b8-449c-a6ba-88f9c5020556-DsTmpGrp"),
				},
			},
			users: []*testUser{
				{
					user: &iam.User{
						Arn:      aws.String("arn:aws:iam::12345678901:users/dataset-091faadd-b8b8-449c-a6ba-88f9c5020556-DsTmpUsr"),
						UserName: aws.String("dataset-091faadd-b8b8-449c-a6ba-88f9c5020556-DsTmpUsr"),
					},
					accessKeys: []*iam.AccessKeyMetadata{},
				},
			},
			err: errors.New("InternalError: listing user access keys for dataset 091faadd-b8b8-449c-a6ba-88f9c5020556 (TestUpdateUser ListAccessKeysWithContext)"),
			merr: map[string]error{
				"ListAccessKeysWithContext": errors.New("TestUpdateUser ListAccessKeysWithContext"),
			},
		},
		{
			id: "9e0f9251-9672-40dc-85b0-05cc2e146f30",
			group: &testGroup{
				group: &iam.Group{
					GroupName: aws.String("dataset-9e0f9251-9672-40dc-85b0-05cc2e146f30-DsTmpGrp"),
				},
			},
			users: []*testUser{
				{
					user: &iam.User{
						Arn:      aws.String("arn:aws:iam::12345678901:users/dataset-9e0f9251-9672-40dc-85b0-05cc2e146f30-DsTmpUsr"),
						UserName: aws.String("dataset-9e0f9251-9672-40dc-85b0-05cc2e146f30-DsTmpUsr"),
					},
					accessKeys: []*iam.AccessKeyMetadata{},
				},
			},
			err: errors.New("InternalError: create user access key for dataset 9e0f9251-9672-40dc-85b0-05cc2e146f30 (TestUpdateUser CreateAccessKeyWithContext)"),
			merr: map[string]error{
				"CreateAccessKeyWithContext": errors.New("TestUpdateUser CreateAccessKeyWithContext"),
			},
		},
		{
			id: "8b17eea2-2afe-4eaf-9c1f-efe0f210c387",
			group: &testGroup{
				group: &iam.Group{
					GroupName: aws.String("dataset-8b17eea2-2afe-4eaf-9c1f-efe0f210c387-DsTmpGrp"),
				},
			},
			users: []*testUser{
				{
					user: &iam.User{
						Arn:      aws.String("arn:aws:iam::12345678901:users/dataset-8b17eea2-2afe-4eaf-9c1f-efe0f210c387-DsTmpUsr"),
						UserName: aws.String("dataset-8b17eea2-2afe-4eaf-9c1f-efe0f210c387-DsTmpUsr"),
					},
					accessKeys: []*iam.AccessKeyMetadata{
						{
							AccessKeyId: aws.String("OPQRSTUV"),
							Status:      aws.String("Active"),
						},
					},
				},
			},
			err: errors.New("InternalError: deactivating user access key for dataset 8b17eea2-2afe-4eaf-9c1f-efe0f210c387 (TestUpdateUser UpdateAccessKeyWithContext)"),
			merr: map[string]error{
				"UpdateAccessKeyWithContext": errors.New("TestUpdateUser UpdateAccessKeyWithContext"),
			},
		},
	}
	testDatasets = append(newTestDatasets(), updateUserErrTests...)

	t.Logf("length of tests: %d", len(testDatasets))

	for _, tds := range testDatasets {
		s := newTestS3Repository(t)
		if tds.merr != nil {
			s.IAM.(*mockIAMClient).err = tds.merr
			if _, err := s.UpdateUser(context.TODO(), tds.id); err != nil {
				if err.Error() != tds.err.Error() {
					t.Errorf("expected error '%s', got '%s'", tds.err, err)
				}
			} else {
				t.Error("expected error, got nil")
			}
			continue
		}

		var expected map[string]interface{}
		for _, u := range tds.users {
			keyid := aws.StringValue(u.user.UserName) + "KEY"
			secret := aws.StringValue(u.user.UserName) + "SECRET"
			keys := map[string]string{}
			for _, k := range u.accessKeys {
				keys[aws.StringValue(k.AccessKeyId)] = "Inactive"
			}

			if len(u.accessKeys) < 2 {
				expected = map[string]interface{}{
					"keys": keys,
					"credentials": struct {
						KeyId  string `json:"akid"`
						Secret string `json:"secret"`
					}{
						KeyId:  keyid,
						Secret: secret,
					},
				}
			} else {
				active := 0
				for _, k := range u.accessKeys {
					if aws.StringValue(k.Status) == "Active" {
						active += 1
					}
				}

				if active == 0 {
					tds.err = errors.New("LimitExceeded: too many access keys (2)")
				} else {
					expected = map[string]interface{}{"keys": keys}
				}

			}
		}

		out, err := s.UpdateUser(context.TODO(), tds.id)
		if err != nil && tds.err == nil {
			t.Errorf("expected nil error, got %s", err)
		} else if err == nil && tds.err != nil {
			t.Errorf("expected err %s, got nil", tds.err)
		} else if err != nil {
			if tds.err.Error() != err.Error() {
				t.Errorf("expected error %s, got %s", tds.err, err)
			}
		} else {
			t.Logf("got output %+v", out)

			if !reflect.DeepEqual(expected, out) {
				t.Errorf("expected %+v, got %+v", expected, out)
			}
		}
	}
}
