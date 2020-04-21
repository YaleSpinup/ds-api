package s3datarepository

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/YaleSpinup/ds-api/apierror"
	"github.com/YaleSpinup/ds-api/dataset"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/ec2/ec2iface"
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/aws/aws-sdk-go/service/iam/iamiface"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/aws/aws-sdk-go/service/sts/stsiface"
)

// mockEC2Client is a fake EC2 client
type mockEC2Client struct {
	ec2iface.EC2API
	t   *testing.T
	err map[string]error
}

// mockIAMClient is a fake IAM client
type mockIAMClient struct {
	iamiface.IAMAPI
	t   *testing.T
	err map[string]error
}

// mockSTSClient is a fake STS client
type mockSTSClient struct {
	stsiface.STSAPI
	t *testing.T
}

func newMockEC2Client(t *testing.T) ec2iface.EC2API {
	return &mockEC2Client{
		t:   t,
		err: make(map[string]error),
	}
}

func newMockIAMClient(t *testing.T) iamiface.IAMAPI {
	return &mockIAMClient{
		t:   t,
		err: make(map[string]error),
	}
}

func newMockSTSClient(t *testing.T) stsiface.STSAPI {
	return &mockSTSClient{
		t: t,
	}
}

func (c *mockEC2Client) AssociateIamInstanceProfileWithContext(ctx context.Context, input *ec2.AssociateIamInstanceProfileInput, opts ...request.Option) (*ec2.AssociateIamInstanceProfileOutput, error) {
	if err, ok := c.err["AssociateIamInstanceProfileWithContext"]; ok {
		return nil, err
	}

	return &ec2.AssociateIamInstanceProfileOutput{}, nil
}

func (c *mockEC2Client) DescribeIamInstanceProfileAssociationsWithContext(ctx context.Context, input *ec2.DescribeIamInstanceProfileAssociationsInput, opts ...request.Option) (*ec2.DescribeIamInstanceProfileAssociationsOutput, error) {
	if err, ok := c.err["DescribeIamInstanceProfileAssociationsWithContext"]; ok {
		return nil, err
	}

	var instanceID, state *string
	for i := range input.Filters {
		if aws.StringValue(input.Filters[i].Name) == "instance_id" {
			instanceID = input.Filters[i].Values[0]
		}
		if aws.StringValue(input.Filters[i].Name) == "state" {
			state = input.Filters[i].Values[0]
		}
	}

	output := &ec2.DescribeIamInstanceProfileAssociationsOutput{
		IamInstanceProfileAssociations: []*ec2.IamInstanceProfileAssociation{
			&ec2.IamInstanceProfileAssociation{
				AssociationId: aws.String("iip-assoc-01eeadf75d9ccb1b7"),
				IamInstanceProfile: &ec2.IamInstanceProfile{
					Arn: aws.String("arn:aws:iam::12345678901:instance-profile/someProfile"),
					Id:  aws.String("AIPAXQVXYEBXBUQJQCVBX"),
				},
				InstanceId: instanceID,
				State:      state,
				Timestamp:  &testTime,
			},
		},
	}

	return output, nil
}

func (c *mockEC2Client) DescribeInstancesWithContext(ctx context.Context, input *ec2.DescribeInstancesInput, opts ...request.Option) (*ec2.DescribeInstancesOutput, error) {
	if err, ok := c.err["DescribeInstancesWithContext"]; ok {
		return nil, err
	}

	var output *ec2.DescribeInstancesOutput

	if input.InstanceIds != nil {
		instanceID := input.InstanceIds[0]

		if aws.StringValue(instanceID) == "i-0123456789abcdef1" {
			// does not have instance profile
			output = &ec2.DescribeInstancesOutput{
				Reservations: []*ec2.Reservation{
					&ec2.Reservation{
						Instances: []*ec2.Instance{
							&ec2.Instance{
								InstanceId: instanceID,
							},
						},
					},
				},
			}
		} else if aws.StringValue(instanceID) == "i-0123456789abcdef2" {
			// has an instance profile (but not the instanceRole one)
			output = &ec2.DescribeInstancesOutput{
				Reservations: []*ec2.Reservation{
					&ec2.Reservation{
						Instances: []*ec2.Instance{
							&ec2.Instance{
								IamInstanceProfile: &ec2.IamInstanceProfile{
									Arn: aws.String(fmt.Sprintf("arn:aws:iam::12345678901:instance-profile/theOtherRole")),
									Id:  aws.String("AIPAXQVXYEBXNT646ERL7"),
								},
								InstanceId: instanceID,
							},
						},
					},
				},
			}
		} else if aws.StringValue(instanceID) == "i-0123456789abcdef3" {
			// has an instance profile (the instanceRole one)
			output = &ec2.DescribeInstancesOutput{
				Reservations: []*ec2.Reservation{
					&ec2.Reservation{
						Instances: []*ec2.Instance{
							&ec2.Instance{
								IamInstanceProfile: &ec2.IamInstanceProfile{
									Arn: aws.String(fmt.Sprintf("arn:aws:iam::12345678901:instance-profile/test/instanceRole_%s", aws.StringValue(instanceID))),
									Id:  aws.String("AIPAXQVXYEBXNT646ERL6"),
								},
								InstanceId: instanceID,
							},
						},
					},
				},
			}
		} else {
			output = &ec2.DescribeInstancesOutput{}
		}
	} else if input.Filters != nil {
		var instanceProfileArn string
		var instanceID *string

		for i := range input.Filters {
			if aws.StringValue(input.Filters[i].Name) == "iam-instance-profile.arn" {
				instanceProfileArn = aws.StringValue(input.Filters[i].Values[0])
			}
		}
		switch instanceProfileArn {
		case "arn:aws:iam::12345678901:instance-profile/test/instanceRole_i-0123456789abcdef1":
			instanceID = aws.String("i-0123456789abcdef1")
		case "arn:aws:iam::12345678901:instance-profile/test/instanceRole_i-0123456789abcdef2":
			instanceID = aws.String("i-0123456789abcdef2")
		case "arn:aws:iam::12345678901:instance-profile/test/instanceRole_i-0123456789abcdef3":
			instanceID = aws.String("i-0123456789abcdef3")
		default:
			instanceID = aws.String("i-NA")
		}

		output = &ec2.DescribeInstancesOutput{
			Reservations: []*ec2.Reservation{
				&ec2.Reservation{
					Instances: []*ec2.Instance{
						&ec2.Instance{
							IamInstanceProfile: &ec2.IamInstanceProfile{
								Arn: aws.String(fmt.Sprintf("arn:aws:iam::12345678901:instance-profile/test/instanceRole_%s", aws.StringValue(instanceID))),
								Id:  aws.String("AIPAXQVXYEBXNT646ERL6"),
							},
							InstanceId: instanceID,
						},
					},
				},
			},
		}
	} else {
		output = &ec2.DescribeInstancesOutput{}
	}

	return output, nil
}

func (c *mockEC2Client) DisassociateIamInstanceProfileWithContext(ctx context.Context, input *ec2.DisassociateIamInstanceProfileInput, opts ...request.Option) (*ec2.DisassociateIamInstanceProfileOutput, error) {
	if err, ok := c.err["DisassociateIamInstanceProfileWithContext"]; ok {
		return nil, err
	}

	return &ec2.DisassociateIamInstanceProfileOutput{}, nil
}

func (i *mockIAMClient) AddRoleToInstanceProfileWithContext(ctx context.Context, input *iam.AddRoleToInstanceProfileInput, opts ...request.Option) (*iam.AddRoleToInstanceProfileOutput, error) {
	if err, ok := i.err["AddRoleToInstanceProfileWithContext"]; ok {
		return nil, err
	}
	return &iam.AddRoleToInstanceProfileOutput{}, nil
}

func (i *mockIAMClient) AttachRolePolicyWithContext(ctx context.Context, input *iam.AttachRolePolicyInput, opts ...request.Option) (*iam.AttachRolePolicyOutput, error) {
	if err, ok := i.err["AttachRolePolicyWithContext"]; ok {
		return nil, err
	}
	return &iam.AttachRolePolicyOutput{}, nil
}

func (i *mockIAMClient) CreateInstanceProfileWithContext(ctx context.Context, input *iam.CreateInstanceProfileInput, opts ...request.Option) (*iam.CreateInstanceProfileOutput, error) {
	if err, ok := i.err["CreateInstanceProfileWithContext"]; ok {
		return nil, err
	}

	output := &iam.CreateInstanceProfileOutput{InstanceProfile: &iam.InstanceProfile{
		Arn:                 aws.String(fmt.Sprintf("arn:aws:iam::12345678901:instance-profile%s%s", *input.Path, *input.InstanceProfileName)),
		CreateDate:          &testTime,
		Path:                input.Path,
		InstanceProfileId:   aws.String(strings.ToUpper(fmt.Sprintf("%sID123", *input.InstanceProfileName))),
		InstanceProfileName: input.InstanceProfileName,
	}}

	return output, nil
}

func (i *mockIAMClient) CreatePolicyWithContext(ctx context.Context, input *iam.CreatePolicyInput, opts ...request.Option) (*iam.CreatePolicyOutput, error) {
	if err, ok := i.err["CreatePolicyWithContext"]; ok {
		return nil, err
	}

	output := &iam.Policy{
		Arn:                           aws.String(fmt.Sprintf("arn:aws:iam::12345678901:policy%s%s", *input.Path, *input.PolicyName)),
		AttachmentCount:               aws.Int64(0),
		CreateDate:                    &testTime,
		DefaultVersionId:              aws.String("v1"),
		Description:                   aws.String("policy thang"),
		IsAttachable:                  aws.Bool(true),
		Path:                          input.Path,
		PermissionsBoundaryUsageCount: aws.Int64(0),
		PolicyId:                      aws.String("TESTPOLICYID123"),
		PolicyName:                    input.PolicyName,
		UpdateDate:                    &testTime,
	}

	return &iam.CreatePolicyOutput{Policy: output}, nil
}

func (i *mockIAMClient) CreateRoleWithContext(ctx context.Context, input *iam.CreateRoleInput, opts ...request.Option) (*iam.CreateRoleOutput, error) {
	if err, ok := i.err["CreateRoleWithContext"]; ok {
		return nil, err
	}

	output := &iam.CreateRoleOutput{Role: &iam.Role{
		Arn:         aws.String(fmt.Sprintf("arn:aws:iam::12345678901:role%s%s", *input.Path, *input.RoleName)),
		CreateDate:  &testTime,
		Description: input.Description,
		Path:        input.Path,
		RoleId:      aws.String(strings.ToUpper(fmt.Sprintf("%sID123", *input.RoleName))),
		RoleName:    input.RoleName,
	}}

	return output, nil
}

func (i *mockIAMClient) DeleteInstanceProfileWithContext(ctx context.Context, input *iam.DeleteInstanceProfileInput, opts ...request.Option) (*iam.DeleteInstanceProfileOutput, error) {
	if err, ok := i.err["DeleteInstanceProfileWithContext"]; ok {
		return nil, err
	}
	return &iam.DeleteInstanceProfileOutput{}, nil
}

func (i *mockIAMClient) DeletePolicyWithContext(ctx context.Context, input *iam.DeletePolicyInput, opts ...request.Option) (*iam.DeletePolicyOutput, error) {
	if err, ok := i.err["DeletePolicyWithContext"]; ok {
		return nil, err
	}
	return &iam.DeletePolicyOutput{}, nil
}

func (i *mockIAMClient) DeleteRoleWithContext(ctx context.Context, input *iam.DeleteRoleInput, opts ...request.Option) (*iam.DeleteRoleOutput, error) {
	if err, ok := i.err["DeleteRoleWithContext"]; ok {
		return nil, err
	}
	return &iam.DeleteRoleOutput{}, nil
}

func (i *mockIAMClient) DetachRolePolicyWithContext(ctx context.Context, input *iam.DetachRolePolicyInput, opts ...request.Option) (*iam.DetachRolePolicyOutput, error) {
	if err, ok := i.err["DetachRolePolicyWithContext"]; ok {
		return nil, err
	}
	return &iam.DetachRolePolicyOutput{}, nil
}

func (i *mockIAMClient) GetInstanceProfileWithContext(ctx context.Context, input *iam.GetInstanceProfileInput, opts ...request.Option) (*iam.GetInstanceProfileOutput, error) {
	if err, ok := i.err["GetInstanceProfileWithContext"]; ok {
		return nil, err
	}

	output := &iam.GetInstanceProfileOutput{
		InstanceProfile: &iam.InstanceProfile{
			Arn:                 aws.String(fmt.Sprintf("arn:aws:iam::12345678901:instance-profile%s%s", "/test/", *input.InstanceProfileName)),
			CreateDate:          &testTime,
			Path:                aws.String("/test/"),
			InstanceProfileId:   aws.String(strings.ToUpper(fmt.Sprintf("%sID123", *input.InstanceProfileName))),
			InstanceProfileName: input.InstanceProfileName,
			Roles: []*iam.Role{
				&iam.Role{
					Arn:         aws.String(fmt.Sprintf("arn:aws:iam::12345678901:role%s%s", "/test/", *input.InstanceProfileName)),
					CreateDate:  &testTime,
					Description: aws.String("Test role"),
					Path:        aws.String("/test/"),
					RoleId:      aws.String(strings.ToUpper(fmt.Sprintf("%sID123", *input.InstanceProfileName))),
					RoleName:    input.InstanceProfileName,
				},
			},
		},
	}

	return output, nil
}

func (i *mockIAMClient) GetRoleWithContext(ctx context.Context, input *iam.GetRoleInput, opts ...request.Option) (*iam.GetRoleOutput, error) {
	if err, ok := i.err["GetRoleWithContext"]; ok {
		return nil, err
	}

	output := &iam.GetRoleOutput{Role: &iam.Role{
		Arn:         aws.String(fmt.Sprintf("arn:aws:iam::12345678901:role%s%s", "/test/", *input.RoleName)),
		CreateDate:  &testTime,
		Description: aws.String("Test role"),
		Path:        aws.String("/test/"),
		RoleId:      aws.String(strings.ToUpper(fmt.Sprintf("%sID123", *input.RoleName))),
		RoleName:    input.RoleName,
	}}

	return output, nil
}

func (i *mockIAMClient) ListAttachedRolePoliciesWithContext(ctx context.Context, input *iam.ListAttachedRolePoliciesInput, opts ...request.Option) (*iam.ListAttachedRolePoliciesOutput, error) {
	if err, ok := i.err["ListAttachedRolePoliciesWithContext"]; ok {
		return nil, err
	}

	var output *iam.ListAttachedRolePoliciesOutput
	if aws.StringValue(input.RoleName) == "instanceRole_i-0123456789abcdef3" {
		output = &iam.ListAttachedRolePoliciesOutput{
			AttachedPolicies: []*iam.AttachedPolicy{
				&iam.AttachedPolicy{
					PolicyArn:  aws.String("arn:aws:iam::12345678901:policy/test/dataset-BF155F4A-A464-4D4D-A948-BF1E1E882C6F"),
					PolicyName: aws.String("dataset-BF155F4A-A464-4D4D-A948-BF1E1E882C6F"),
				},
			},
		}
	} else if aws.StringValue(input.RoleName) == "instanceRole_i-0123456789abcdef2" {
		output = &iam.ListAttachedRolePoliciesOutput{
			AttachedPolicies: []*iam.AttachedPolicy{
				&iam.AttachedPolicy{
					PolicyArn:  aws.String("arn:aws:iam::12345678901:policy/test/theOtherPolicy"),
					PolicyName: aws.String("theOtherPolicy"),
				},
			},
		}
	} else {
		output = &iam.ListAttachedRolePoliciesOutput{
			AttachedPolicies: []*iam.AttachedPolicy{},
		}
	}

	return output, nil
}

func (i *mockIAMClient) ListInstanceProfilesForRoleWithContext(ctx context.Context, input *iam.ListInstanceProfilesForRoleInput, opts ...request.Option) (*iam.ListInstanceProfilesForRoleOutput, error) {
	if err, ok := i.err["ListInstanceProfilesForRoleWithContext"]; ok {
		return nil, err
	}

	return &iam.ListInstanceProfilesForRoleOutput{
		InstanceProfiles: []*iam.InstanceProfile{
			&iam.InstanceProfile{
				Arn:                 aws.String(fmt.Sprintf("arn:aws:iam::12345678901:instance-profile%s%s", "/test/", *input.RoleName)),
				InstanceProfileName: input.RoleName,
			},
		},
	}, nil
}

func (i *mockIAMClient) ListEntitiesForPolicyWithContext(ctx context.Context, input *iam.ListEntitiesForPolicyInput, opts ...request.Option) (*iam.ListEntitiesForPolicyOutput, error) {
	if err, ok := i.err["ListEntitiesForPolicyWithContext"]; ok {
		return nil, err
	}

	var output *iam.ListEntitiesForPolicyOutput

	if strings.Contains(aws.StringValue(input.PolicyArn), "DATASET-POLICY-NOT-USED") {
		output = &iam.ListEntitiesForPolicyOutput{
			PolicyRoles: []*iam.PolicyRole{},
		}
	} else {
		output = &iam.ListEntitiesForPolicyOutput{
			PolicyRoles: []*iam.PolicyRole{
				&iam.PolicyRole{
					RoleName: aws.String("instanceRole_i-0123456789abcdef3"),
				},
			},
		}
	}

	return output, nil
}

func (i *mockIAMClient) RemoveRoleFromInstanceProfileWithContext(ctx context.Context, input *iam.RemoveRoleFromInstanceProfileInput, opts ...request.Option) (*iam.RemoveRoleFromInstanceProfileOutput, error) {
	if err, ok := i.err["RemoveRoleFromInstanceProfileWithContext"]; ok {
		return nil, err
	}
	return &iam.RemoveRoleFromInstanceProfileOutput{}, nil
}

func (s *mockSTSClient) GetCallerIdentityWithContext(ctx context.Context, input *sts.GetCallerIdentityInput, opts ...request.Option) (*sts.GetCallerIdentityOutput, error) {
	return &sts.GetCallerIdentityOutput{
		Account: aws.String("12345678901"),
		Arn:     aws.String("arn:aws:iam::12345678901:user/test"),
		UserId:  aws.String("test"),
	}, nil
}

func newTestS3Repository(t *testing.T) S3Repository {
	return S3Repository{
		NamePrefix:    "dataset",
		IAMPathPrefix: "/test/",
		EC2:           newMockEC2Client(t),
		IAM:           newMockIAMClient(t),
		S3:            newMockS3Client(t),
		STS:           newMockSTSClient(t),
	}
}
func TestGrantAccess(t *testing.T) {
	var expectedCode, expectedMessage, id, instanceID string
	var s S3Repository

	id = "78DAFEF1-E4D3-48E5-A45C-6E3CA0161F08"

	// we have a couple test instances:
	// i-0123456789abcdef1 - does _not_ have a currently associated instance profile
	// i-0123456789abcdef2 - has a currently associated instance profile

	// test success, instance role exists, instance does not have a profile attached
	s = newTestS3Repository(t)
	instanceID = "i-0123456789abcdef1"
	expected := dataset.Access{
		instanceID: fmt.Sprintf("instanceRole_%s", instanceID),
	}
	got, err := s.GrantAccess(context.TODO(), id, instanceID)
	if err != nil {
		t.Errorf("expected nil error, got: %s", err)
	}
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("expected output:\n%+v, got:\n%+v", expected, got)
	}

	// test success, instance role does not exist, instance does not have a profile attached
	s = newTestS3Repository(t)
	instanceID = "i-0123456789abcdef1"
	expected = dataset.Access{
		instanceID: fmt.Sprintf("instanceRole_%s", instanceID),
	}
	s.IAM.(*mockIAMClient).err["GetRoleWithContext"] = awserr.New(iam.ErrCodeNoSuchEntityException, "ErrCodeNoSuchEntityException", nil)
	got, err = s.GrantAccess(context.TODO(), id, instanceID)
	if err != nil {
		t.Errorf("expected nil error, got: %s", err)
	}
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("expected output:\n%+v, got:\n%+v", expected, got)
	}

	// test success, instance role exists, instance has a profile attached
	s = newTestS3Repository(t)
	instanceID = "i-0123456789abcdef2"
	expected = dataset.Access{
		instanceID: fmt.Sprintf("instanceRole_%s", instanceID),
	}
	got, err = s.GrantAccess(context.TODO(), id, instanceID)
	if err != nil {
		t.Errorf("expected nil error, got: %s", err)
	}
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("expected output:\n%+v, got:\n%+v", expected, got)
	}

	// test success, instance role does not exist, instance has a profile attached
	s = newTestS3Repository(t)
	instanceID = "i-0123456789abcdef2"
	expected = dataset.Access{
		instanceID: fmt.Sprintf("instanceRole_%s", instanceID),
	}
	s.IAM.(*mockIAMClient).err["GetRoleWithContext"] = awserr.New(iam.ErrCodeNoSuchEntityException, "ErrCodeNoSuchEntityException", nil)
	got, err = s.GrantAccess(context.TODO(), id, instanceID)
	if err != nil {
		t.Errorf("expected nil error, got: %s", err)
	}
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("expected output:\n%+v, got:\n%+v", expected, got)
	}

	// test error cases

	// test empty id
	s = newTestS3Repository(t)
	instanceID = "i-0123456789abcdef1"
	_, err = s.GrantAccess(context.TODO(), "", instanceID)
	if aerr, ok := err.(apierror.Error); ok {
		if aerr.Code != apierror.ErrBadRequest {
			t.Errorf("expected error code %s, got: %s", apierror.ErrBadRequest, aerr.Code)
		}
	} else {
		t.Errorf("expected apierror.Error, got: %s", reflect.TypeOf(err).String())
	}

	// test empty instanceID
	s = newTestS3Repository(t)
	_, err = s.GrantAccess(context.TODO(), id, "")
	if aerr, ok := err.(apierror.Error); ok {
		if aerr.Code != apierror.ErrBadRequest {
			t.Errorf("expected error code %s, got: %s", apierror.ErrBadRequest, aerr.Code)
		}
	} else {
		t.Errorf("expected apierror.Error, got: %s", reflect.TypeOf(err).String())
	}

	// test DescribeInstancesWithContext failure
	s = newTestS3Repository(t)
	instanceID = "i-0123456789abcdef1"
	expectedCode = apierror.ErrServiceUnavailable
	expectedMessage = fmt.Sprintf("failed to get information about instance %s", instanceID)
	s.EC2.(*mockEC2Client).err["DescribeInstancesWithContext"] = awserr.New("InternalError", "Internal Error", nil)

	_, err = s.GrantAccess(context.TODO(), id, instanceID)
	if err == nil {
		t.Error("expected error, got: nil")
	} else {
		if aerr, ok := err.(apierror.Error); ok {
			if aerr.Code != expectedCode {
				t.Errorf("expected error code %s, got: %s", expectedCode, aerr.Code)
			}
			if aerr.Message != expectedMessage {
				t.Errorf("expected error message '%s', got: '%s'", expectedMessage, aerr.Message)
			}
		} else {
			t.Errorf("expected apierror.Error, got: %s", reflect.TypeOf(err).String())
		}
	}

	// test GetRoleWithContext failure
	s = newTestS3Repository(t)
	instanceID = "i-0123456789abcdef1"
	expectedCode = apierror.ErrServiceUnavailable
	expectedMessage = fmt.Sprintf("failed to get IAM role instanceRole_%s", instanceID)
	s.IAM.(*mockIAMClient).err["GetRoleWithContext"] = awserr.New("InternalError", "Internal Error", nil)

	_, err = s.GrantAccess(context.TODO(), id, instanceID)
	if err == nil {
		t.Error("expected error, got: nil")
	} else {
		if aerr, ok := err.(apierror.Error); ok {
			if aerr.Code != expectedCode {
				t.Errorf("expected error code %s, got: %s", expectedCode, aerr.Code)
			}
			if aerr.Message != expectedMessage {
				t.Errorf("expected error message '%s', got: '%s'", expectedMessage, aerr.Message)
			}
		} else {
			t.Errorf("expected apierror.Error, got: %s", reflect.TypeOf(err).String())
		}
	}

	// test CreateRoleWithContext failure
	s = newTestS3Repository(t)
	instanceID = "i-0123456789abcdef1"
	expectedCode = apierror.ErrServiceUnavailable
	expectedMessage = fmt.Sprintf("failed to create IAM role instanceRole_%s", instanceID)
	s.IAM.(*mockIAMClient).err["GetRoleWithContext"] = awserr.New(iam.ErrCodeNoSuchEntityException, "ErrCodeNoSuchEntityException", nil)
	s.IAM.(*mockIAMClient).err["CreateRoleWithContext"] = awserr.New("InternalError", "Internal Error", nil)

	_, err = s.GrantAccess(context.TODO(), id, instanceID)
	if err == nil {
		t.Error("expected error, got: nil")
	} else {
		if aerr, ok := err.(apierror.Error); ok {
			if aerr.Code != expectedCode {
				t.Errorf("expected error code %s, got: %s", expectedCode, aerr.Code)
			}
			if aerr.Message != expectedMessage {
				t.Errorf("expected error message '%s', got: '%s'", expectedMessage, aerr.Message)
			}
		} else {
			t.Errorf("expected apierror.Error, got: %s", reflect.TypeOf(err).String())
		}
	}

	// test CreateInstanceProfileWithContext failure
	s = newTestS3Repository(t)
	instanceID = "i-0123456789abcdef1"
	expectedCode = apierror.ErrServiceUnavailable
	expectedMessage = fmt.Sprintf("failed to create instance profile instanceRole_%s", instanceID)
	s.IAM.(*mockIAMClient).err["GetRoleWithContext"] = awserr.New(iam.ErrCodeNoSuchEntityException, "ErrCodeNoSuchEntityException", nil)
	s.IAM.(*mockIAMClient).err["CreateInstanceProfileWithContext"] = awserr.New("InternalError", "Internal Error", nil)

	_, err = s.GrantAccess(context.TODO(), id, instanceID)
	if err == nil {
		t.Error("expected error, got: nil")
	} else {
		if aerr, ok := err.(apierror.Error); ok {
			if aerr.Code != expectedCode {
				t.Errorf("expected error code %s, got: %s", expectedCode, aerr.Code)
			}
			if aerr.Message != expectedMessage {
				t.Errorf("expected error message '%s', got: '%s'", expectedMessage, aerr.Message)
			}
		} else {
			t.Errorf("expected apierror.Error, got: %s", reflect.TypeOf(err).String())
		}
	}

	// test AddRoleToInstanceProfileWithContext failure
	s = newTestS3Repository(t)
	instanceID = "i-0123456789abcdef1"
	expectedCode = apierror.ErrServiceUnavailable
	expectedMessage = fmt.Sprintf("failed to add role to instance profile instanceRole_%s", instanceID)
	s.IAM.(*mockIAMClient).err["GetRoleWithContext"] = awserr.New(iam.ErrCodeNoSuchEntityException, "ErrCodeNoSuchEntityException", nil)
	s.IAM.(*mockIAMClient).err["AddRoleToInstanceProfileWithContext"] = awserr.New("InternalError", "Internal Error", nil)

	_, err = s.GrantAccess(context.TODO(), id, instanceID)
	if err == nil {
		t.Error("expected error, got: nil")
	} else {
		if aerr, ok := err.(apierror.Error); ok {
			if aerr.Code != expectedCode {
				t.Errorf("expected error code %s, got: %s", expectedCode, aerr.Code)
			}
			if aerr.Message != expectedMessage {
				t.Errorf("expected error message '%s', got: '%s'", expectedMessage, aerr.Message)
			}
		} else {
			t.Errorf("expected apierror.Error, got: %s", reflect.TypeOf(err).String())
		}
	}

	// test AttachRolePolicyWithContext failure
	s = newTestS3Repository(t)
	instanceID = "i-0123456789abcdef1"
	expectedCode = apierror.ErrServiceUnavailable
	expectedMessage = fmt.Sprintf("failed to attach policy arn:aws:iam::12345678901:policy/test/dataset-%s to role instanceRole_%s", id, instanceID)
	s.IAM.(*mockIAMClient).err["AttachRolePolicyWithContext"] = awserr.New("InternalError", "Internal Error", nil)

	_, err = s.GrantAccess(context.TODO(), id, instanceID)
	if err == nil {
		t.Error("expected error, got: nil")
	} else {
		if aerr, ok := err.(apierror.Error); ok {
			if aerr.Code != expectedCode {
				t.Errorf("expected error code %s, got: %s", expectedCode, aerr.Code)
			}
			if aerr.Message != expectedMessage {
				t.Errorf("expected error message '%s', got: '%s'", expectedMessage, aerr.Message)
			}
		} else {
			t.Errorf("expected apierror.Error, got: %s", reflect.TypeOf(err).String())
		}
	}

	// test GetInstanceProfileWithContext failure
	s = newTestS3Repository(t)
	instanceID = "i-0123456789abcdef2"
	expectedCode = apierror.ErrServiceUnavailable
	expectedMessage = fmt.Sprintf("failed to get information about current instance profile theOtherRole")
	s.IAM.(*mockIAMClient).err["GetInstanceProfileWithContext"] = awserr.New("InternalError", "Internal Error", nil)

	_, err = s.GrantAccess(context.TODO(), id, instanceID)
	if err == nil {
		t.Error("expected error, got: nil")
	} else {
		if aerr, ok := err.(apierror.Error); ok {
			if aerr.Code != expectedCode {
				t.Errorf("expected error code %s, got: %s", expectedCode, aerr.Code)
			}
			if aerr.Message != expectedMessage {
				t.Errorf("expected error message '%s', got: '%s'", expectedMessage, aerr.Message)
			}
		} else {
			t.Errorf("expected apierror.Error, got: %s", reflect.TypeOf(err).String())
		}
	}

	// test ListAttachedRolePoliciesWithContext failure
	s = newTestS3Repository(t)
	instanceID = "i-0123456789abcdef2"
	expectedCode = apierror.ErrServiceUnavailable
	expectedMessage = fmt.Sprintf("failed to list attached policies for role theOtherRole")
	s.IAM.(*mockIAMClient).err["ListAttachedRolePoliciesWithContext"] = awserr.New("InternalError", "Internal Error", nil)

	_, err = s.GrantAccess(context.TODO(), id, instanceID)
	if err == nil {
		t.Error("expected error, got: nil")
	} else {
		if aerr, ok := err.(apierror.Error); ok {
			if aerr.Code != expectedCode {
				t.Errorf("expected error code %s, got: %s", expectedCode, aerr.Code)
			}
			if aerr.Message != expectedMessage {
				t.Errorf("expected error message '%s', got: '%s'", expectedMessage, aerr.Message)
			}
		} else {
			t.Errorf("expected apierror.Error, got: %s", reflect.TypeOf(err).String())
		}
	}

	// test AttachRolePolicyWithContext failure
	s = newTestS3Repository(t)
	instanceID = "i-0123456789abcdef2"
	expectedCode = apierror.ErrServiceUnavailable
	expectedMessage = fmt.Sprintf("failed to attach policy arn:aws:iam::12345678901:policy/test/dataset-%s to role instanceRole_%s", id, instanceID)
	s.IAM.(*mockIAMClient).err["AttachRolePolicyWithContext"] = awserr.New("InternalError", "Internal Error", nil)

	_, err = s.GrantAccess(context.TODO(), id, instanceID)
	if err == nil {
		t.Error("expected error, got: nil")
	} else {
		if aerr, ok := err.(apierror.Error); ok {
			if aerr.Code != expectedCode {
				t.Errorf("expected error code %s, got: %s", expectedCode, aerr.Code)
			}
			if aerr.Message != expectedMessage {
				t.Errorf("expected error message '%s', got: '%s'", expectedMessage, aerr.Message)
			}
		} else {
			t.Errorf("expected apierror.Error, got: %s", reflect.TypeOf(err).String())
		}
	}

	// test DescribeIamInstanceProfileAssociationsWithContext failure
	s = newTestS3Repository(t)
	instanceID = "i-0123456789abcdef2"
	expectedCode = apierror.ErrServiceUnavailable
	expectedMessage = fmt.Sprintf("failed to describe instance profile associations for instance %s", instanceID)
	s.EC2.(*mockEC2Client).err["DescribeIamInstanceProfileAssociationsWithContext"] = awserr.New("InternalError", "Internal Error", nil)

	_, err = s.GrantAccess(context.TODO(), id, instanceID)
	if err == nil {
		t.Error("expected error, got: nil")
	} else {
		if aerr, ok := err.(apierror.Error); ok {
			if aerr.Code != expectedCode {
				t.Errorf("expected error code %s, got: %s", expectedCode, aerr.Code)
			}
			if aerr.Message != expectedMessage {
				t.Errorf("expected error message '%s', got: '%s'", expectedMessage, aerr.Message)
			}
		} else {
			t.Errorf("expected apierror.Error, got: %s", reflect.TypeOf(err).String())
		}
	}
}

func TestListAccess(t *testing.T) {
	var expectedCode, expectedMessage, id, instanceID string
	// var id, instanceID string
	var s S3Repository

	id = "595AEBB1-431A-4A9E-AE12-2E747F27097F"

	// we have a couple test instances:
	// i-0123456789abcdef1 - does _not_ have a currently associated instance profile
	// i-0123456789abcdef2 - has a currently associated instance profile (without the instanceRole)
	// i-0123456789abcdef3 - has a currently associated instance profile (with the instanceRole)

	// test success - policy attached to one instance
	s = newTestS3Repository(t)
	instanceID = "i-0123456789abcdef3"
	expected := dataset.Access{
		instanceID: fmt.Sprintf("instanceRole_%s", instanceID),
	}
	got, err := s.ListAccess(context.TODO(), id)
	if err != nil {
		t.Errorf("expected nil error, got: %s", err)
	}
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("expected output:\n%+v, got:\n%+v", expected, got)
	}

	// test success - policy not attached anywhere
	s = newTestS3Repository(t)
	expected = dataset.Access{}
	got, err = s.ListAccess(context.TODO(), "DATASET-POLICY-NOT-USED")
	if err != nil {
		t.Errorf("expected nil error, got: %s", err)
	}
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("expected output:\n%+v, got:\n%+v", expected, got)
	}

	// test error cases

	// test empty id
	s = newTestS3Repository(t)
	_, err = s.ListAccess(context.TODO(), "")
	if aerr, ok := err.(apierror.Error); ok {
		if aerr.Code != apierror.ErrBadRequest {
			t.Errorf("expected error code %s, got: %s", apierror.ErrBadRequest, aerr.Code)
		}
	} else {
		t.Errorf("expected apierror.Error, got: %s", reflect.TypeOf(err).String())
	}

	// test ListEntitiesForPolicyWithContext failure
	s = newTestS3Repository(t)
	expectedCode = apierror.ErrServiceUnavailable
	expectedMessage = fmt.Sprintf("failed to list entities for policy arn:aws:iam::12345678901:policy/test/dataset-%s", id)
	s.IAM.(*mockIAMClient).err["ListEntitiesForPolicyWithContext"] = awserr.New("InternalError", "Internal Error", nil)

	_, err = s.ListAccess(context.TODO(), id)
	if err == nil {
		t.Error("expected error, got: nil")
	} else {
		if aerr, ok := err.(apierror.Error); ok {
			if aerr.Code != expectedCode {
				t.Errorf("expected error code %s, got: %s", expectedCode, aerr.Code)
			}
			if aerr.Message != expectedMessage {
				t.Errorf("expected error message '%s', got: '%s'", expectedMessage, aerr.Message)
			}
		} else {
			t.Errorf("expected apierror.Error, got: %s", reflect.TypeOf(err).String())
		}
	}

	// test ListInstanceProfilesForRoleWithContext failure
	s = newTestS3Repository(t)
	expectedCode = apierror.ErrServiceUnavailable
	expectedMessage = fmt.Sprintf("failed to list instance profiles for role instanceRole_i-0123456789abcdef3")
	s.IAM.(*mockIAMClient).err["ListInstanceProfilesForRoleWithContext"] = awserr.New("InternalError", "Internal Error", nil)

	_, err = s.ListAccess(context.TODO(), id)
	if err == nil {
		t.Error("expected error, got: nil")
	} else {
		if aerr, ok := err.(apierror.Error); ok {
			if aerr.Code != expectedCode {
				t.Errorf("expected error code %s, got: %s", expectedCode, aerr.Code)
			}
			if aerr.Message != expectedMessage {
				t.Errorf("expected error message '%s', got: '%s'", expectedMessage, aerr.Message)
			}
		} else {
			t.Errorf("expected apierror.Error, got: %s", reflect.TypeOf(err).String())
		}
	}

	// test DescribeInstancesWithContext failure
	s = newTestS3Repository(t)
	expectedCode = apierror.ErrServiceUnavailable
	expectedMessage = fmt.Sprintf("failed to list instances with instance profile instanceRole_i-0123456789abcdef3")
	s.EC2.(*mockEC2Client).err["DescribeInstancesWithContext"] = awserr.New("InternalError", "Internal Error", nil)

	_, err = s.ListAccess(context.TODO(), id)
	if err == nil {
		t.Error("expected error, got: nil")
	} else {
		if aerr, ok := err.(apierror.Error); ok {
			if aerr.Code != expectedCode {
				t.Errorf("expected error code %s, got: %s", expectedCode, aerr.Code)
			}
			if aerr.Message != expectedMessage {
				t.Errorf("expected error message '%s', got: '%s'", expectedMessage, aerr.Message)
			}
		} else {
			t.Errorf("expected apierror.Error, got: %s", reflect.TypeOf(err).String())
		}
	}
}

func TestRevokeAccess(t *testing.T) {
	var expectedCode, expectedMessage, id, instanceID string
	var s S3Repository

	id = "BF155F4A-A464-4D4D-A948-BF1E1E882C6F"

	// we have a couple test instances:
	// i-0123456789abcdef1 - does _not_ have a currently associated instance profile
	// i-0123456789abcdef2 - has a currently associated instance profile (without the instanceRole)
	// i-0123456789abcdef3 - has a currently associated instance profile (with the instanceRole)

	// test success
	s = newTestS3Repository(t)
	instanceID = "i-0123456789abcdef3"
	err := s.RevokeAccess(context.TODO(), id, instanceID)
	if err != nil {
		t.Errorf("expected nil error, got: %s", err)
	}

	// test error cases

	// test empty id
	s = newTestS3Repository(t)
	instanceID = "i-0123456789abcdef3"
	err = s.RevokeAccess(context.TODO(), "", instanceID)
	if aerr, ok := err.(apierror.Error); ok {
		if aerr.Code != apierror.ErrBadRequest {
			t.Errorf("expected error code %s, got: %s", apierror.ErrBadRequest, aerr.Code)
		}
	} else {
		t.Errorf("expected apierror.Error, got: %s", reflect.TypeOf(err).String())
	}

	// test empty instanceID
	s = newTestS3Repository(t)
	err = s.RevokeAccess(context.TODO(), id, "")
	if aerr, ok := err.(apierror.Error); ok {
		if aerr.Code != apierror.ErrBadRequest {
			t.Errorf("expected error code %s, got: %s", apierror.ErrBadRequest, aerr.Code)
		}
	} else {
		t.Errorf("expected apierror.Error, got: %s", reflect.TypeOf(err).String())
	}

	// test no current access - no policy found
	s = newTestS3Repository(t)
	instanceID = "i-0123456789abcdef2"
	expectedCode = apierror.ErrBadRequest
	expectedMessage = fmt.Sprintf("invalid input")

	err = s.RevokeAccess(context.TODO(), id, instanceID)
	if err == nil {
		t.Error("expected error, got: nil")
	} else {
		if aerr, ok := err.(apierror.Error); ok {
			if aerr.Code != expectedCode {
				t.Errorf("expected error code %s, got: %s", expectedCode, aerr.Code)
			}
			if aerr.Message != expectedMessage {
				t.Errorf("expected error message '%s', got: '%s'", expectedMessage, aerr.Message)
			}
		} else {
			t.Errorf("expected apierror.Error, got: %s", reflect.TypeOf(err).String())
		}
	}

	// test no current access - no instance profile associated
	s = newTestS3Repository(t)
	instanceID = "i-0123456789abcdef1"
	expectedCode = apierror.ErrBadRequest
	expectedMessage = fmt.Sprintf("invalid input")

	err = s.RevokeAccess(context.TODO(), id, instanceID)
	if err == nil {
		t.Error("expected error, got: nil")
	} else {
		if aerr, ok := err.(apierror.Error); ok {
			if aerr.Code != expectedCode {
				t.Errorf("expected error code %s, got: %s", expectedCode, aerr.Code)
			}
			if aerr.Message != expectedMessage {
				t.Errorf("expected error message '%s', got: '%s'", expectedMessage, aerr.Message)
			}
		} else {
			t.Errorf("expected apierror.Error, got: %s", reflect.TypeOf(err).String())
		}
	}

	// test DescribeInstancesWithContext failure
	s = newTestS3Repository(t)
	instanceID = "i-0123456789abcdef3"
	expectedCode = apierror.ErrServiceUnavailable
	expectedMessage = fmt.Sprintf("failed to get information about instance %s", instanceID)
	s.EC2.(*mockEC2Client).err["DescribeInstancesWithContext"] = awserr.New("InternalError", "Internal Error", nil)

	err = s.RevokeAccess(context.TODO(), id, instanceID)
	if err == nil {
		t.Error("expected error, got: nil")
	} else {
		if aerr, ok := err.(apierror.Error); ok {
			if aerr.Code != expectedCode {
				t.Errorf("expected error code %s, got: %s", expectedCode, aerr.Code)
			}
			if aerr.Message != expectedMessage {
				t.Errorf("expected error message '%s', got: '%s'", expectedMessage, aerr.Message)
			}
		} else {
			t.Errorf("expected apierror.Error, got: %s", reflect.TypeOf(err).String())
		}
	}

	// test GetInstanceProfileWithContext failure
	s = newTestS3Repository(t)
	instanceID = "i-0123456789abcdef3"
	expectedCode = apierror.ErrServiceUnavailable
	expectedMessage = fmt.Sprintf("failed to get information about current instance profile instanceRole_%s", instanceID)
	s.IAM.(*mockIAMClient).err["GetInstanceProfileWithContext"] = awserr.New("InternalError", "Internal Error", nil)

	err = s.RevokeAccess(context.TODO(), id, instanceID)
	if err == nil {
		t.Error("expected error, got: nil")
	} else {
		if aerr, ok := err.(apierror.Error); ok {
			if aerr.Code != expectedCode {
				t.Errorf("expected error code %s, got: %s", expectedCode, aerr.Code)
			}
			if aerr.Message != expectedMessage {
				t.Errorf("expected error message '%s', got: '%s'", expectedMessage, aerr.Message)
			}
		} else {
			t.Errorf("expected apierror.Error, got: %s", reflect.TypeOf(err).String())
		}
	}

	// test ListAttachedRolePoliciesWithContext failure
	s = newTestS3Repository(t)
	instanceID = "i-0123456789abcdef3"
	expectedCode = apierror.ErrServiceUnavailable
	expectedMessage = fmt.Sprintf("failed to list attached policies for role instanceRole_%s", instanceID)
	s.IAM.(*mockIAMClient).err["ListAttachedRolePoliciesWithContext"] = awserr.New("InternalError", "Internal Error", nil)

	err = s.RevokeAccess(context.TODO(), id, instanceID)
	if err == nil {
		t.Error("expected error, got: nil")
	} else {
		if aerr, ok := err.(apierror.Error); ok {
			if aerr.Code != expectedCode {
				t.Errorf("expected error code %s, got: %s", expectedCode, aerr.Code)
			}
			if aerr.Message != expectedMessage {
				t.Errorf("expected error message '%s', got: '%s'", expectedMessage, aerr.Message)
			}
		} else {
			t.Errorf("expected apierror.Error, got: %s", reflect.TypeOf(err).String())
		}
	}

	// test DetachRolePolicyWithContext failure
	s = newTestS3Repository(t)
	instanceID = "i-0123456789abcdef3"
	expectedCode = apierror.ErrServiceUnavailable
	expectedMessage = fmt.Sprintf("failed to detach policy arn:aws:iam::12345678901:policy/test/dataset-%s from role instanceRole_%s", id, instanceID)
	s.IAM.(*mockIAMClient).err["DetachRolePolicyWithContext"] = awserr.New("InternalError", "Internal Error", nil)

	err = s.RevokeAccess(context.TODO(), id, instanceID)
	if err == nil {
		t.Error("expected error, got: nil")
	} else {
		if aerr, ok := err.(apierror.Error); ok {
			if aerr.Code != expectedCode {
				t.Errorf("expected error code %s, got: %s", expectedCode, aerr.Code)
			}
			if aerr.Message != expectedMessage {
				t.Errorf("expected error message '%s', got: '%s'", expectedMessage, aerr.Message)
			}
		} else {
			t.Errorf("expected apierror.Error, got: %s", reflect.TypeOf(err).String())
		}
	}

}
