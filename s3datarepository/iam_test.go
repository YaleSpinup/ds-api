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
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/aws/aws-sdk-go/service/iam/iamiface"
)

// mockIAMClient is a fake IAM client
type mockIAMClient struct {
	iamiface.IAMAPI
	t   *testing.T
	err map[string]error
}

func newMockIAMClient(t *testing.T) iamiface.IAMAPI {
	return &mockIAMClient{
		t:   t,
		err: make(map[string]error),
	}
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
		Arn:                 aws.String(fmt.Sprintf("arn:aws:iam::12345678910:instanceprofile%s%s", *input.Path, *input.InstanceProfileName)),
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
		Arn:                           aws.String(fmt.Sprintf("arn:aws:iam::12345678910:policy%s%s", *input.Path, *input.PolicyName)),
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
		Arn:         aws.String(fmt.Sprintf("arn:aws:iam::12345678910:role%s%s", *input.Path, *input.RoleName)),
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

func (i *mockIAMClient) ListAttachedRolePoliciesWithContext(ctx context.Context, input *iam.ListAttachedRolePoliciesInput, opts ...request.Option) (*iam.ListAttachedRolePoliciesOutput, error) {
	if err, ok := i.err["ListAttachedRolePoliciesWithContext"]; ok {
		return nil, err
	}
	return &iam.ListAttachedRolePoliciesOutput{}, nil
}

func (i *mockIAMClient) RemoveRoleFromInstanceProfileWithContext(ctx context.Context, input *iam.RemoveRoleFromInstanceProfileInput, opts ...request.Option) (*iam.RemoveRoleFromInstanceProfileOutput, error) {
	if err, ok := i.err["RemoveRoleFromInstanceProfileWithContext"]; ok {
		return nil, err
	}
	return &iam.RemoveRoleFromInstanceProfileOutput{}, nil
}

func TestGrantAccess(t *testing.T) {
	var expectedCode, expectedMessage, id string

	// test success, derivative false
	s := S3Repository{NamePrefix: "dataset", IAMPathPrefix: "/test/", S3: newMockS3Client(t), IAM: newMockIAMClient(t)}
	id = "78DAFEF1-E4D3-48E5-A45C-6E3CA0161F08"
	expected := &dataset.Access{
		"policy_arn":            fmt.Sprintf("arn:aws:iam::12345678910:policy/test/dataset-%s-OriginalPlc", id),
		"policy_name":           fmt.Sprintf("dataset-%s-OriginalPlc", id),
		"role_arn":              fmt.Sprintf("arn:aws:iam::12345678910:role/test/roleDataset_%s", id),
		"role_name":             fmt.Sprintf("roleDataset_%s", id),
		"instance_profile_arn":  fmt.Sprintf("arn:aws:iam::12345678910:instanceprofile/test/roleDataset_%s", id),
		"instance_profile_name": fmt.Sprintf("roleDataset_%s", id),
	}

	got, err := s.GrantAccess(context.TODO(), id, false)
	if err != nil {
		t.Errorf("expected nil error, got: %s", err)
	}
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("expected output:\n%+v, got:\n%+v", expected, got)
	}

	// test success, derivative true
	s = S3Repository{NamePrefix: "dataset", IAMPathPrefix: "/test/", S3: newMockS3Client(t), IAM: newMockIAMClient(t)}
	id = "78DAFEF1-E4D3-48E5-A45C-6E3CA0161F08"
	expected = &dataset.Access{
		"policy_arn":            fmt.Sprintf("arn:aws:iam::12345678910:policy/test/dataset-%s-DerivativePlc", id),
		"policy_name":           fmt.Sprintf("dataset-%s-DerivativePlc", id),
		"role_arn":              fmt.Sprintf("arn:aws:iam::12345678910:role/test/roleDataset_%s", id),
		"role_name":             fmt.Sprintf("roleDataset_%s", id),
		"instance_profile_arn":  fmt.Sprintf("arn:aws:iam::12345678910:instanceprofile/test/roleDataset_%s", id),
		"instance_profile_name": fmt.Sprintf("roleDataset_%s", id),
	}

	got, err = s.GrantAccess(context.TODO(), id, true)
	if err != nil {
		t.Errorf("expected nil error, got: %s", err)
	}
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("expected output:\n%+v, got:\n%+v", expected, got)
	}

	// test empty id
	s = S3Repository{NamePrefix: "dataset", IAMPathPrefix: "/test/", S3: newMockS3Client(t), IAM: newMockIAMClient(t)}
	_, err = s.GrantAccess(context.TODO(), "", false)
	if aerr, ok := err.(apierror.Error); ok {
		if aerr.Code != apierror.ErrBadRequest {
			t.Errorf("expected error code %s, got: %s", apierror.ErrBadRequest, aerr.Code)
		}
	} else {
		t.Errorf("expected apierror.Error, got: %s", reflect.TypeOf(err).String())
	}

	// test create policy failure
	s = S3Repository{NamePrefix: "dataset", IAMPathPrefix: "/test/", S3: newMockS3Client(t), IAM: newMockIAMClient(t)}
	id = "78DAFEF1-E4D3-48E5-A45C-6E3CA0161F08"
	expectedCode = apierror.ErrServiceUnavailable
	expectedMessage = fmt.Sprintf("failed to create IAM policy")
	s.IAM.(*mockIAMClient).err["CreatePolicyWithContext"] = awserr.New("InternalError", "Internal Error", nil)

	_, err = s.GrantAccess(context.TODO(), id, false)
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

	// test create role failure
	s = S3Repository{NamePrefix: "dataset", IAMPathPrefix: "/test/", S3: newMockS3Client(t), IAM: newMockIAMClient(t)}
	id = "78DAFEF1-E4D3-48E5-A45C-6E3CA0161F08"
	expectedCode = apierror.ErrServiceUnavailable
	expectedMessage = fmt.Sprintf("failed to create IAM role roleDataset_%s", id)
	s.IAM.(*mockIAMClient).err["CreateRoleWithContext"] = awserr.New("InternalError", "Internal Error", nil)

	_, err = s.GrantAccess(context.TODO(), id, false)
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

	// test attach role policy failure
	s = S3Repository{NamePrefix: "dataset", IAMPathPrefix: "/test/", S3: newMockS3Client(t), IAM: newMockIAMClient(t)}
	id = "78DAFEF1-E4D3-48E5-A45C-6E3CA0161F08"
	expectedCode = apierror.ErrServiceUnavailable
	expectedMessage = fmt.Sprintf("failed to attach policy to role roleDataset_%s", id)
	s.IAM.(*mockIAMClient).err["AttachRolePolicyWithContext"] = awserr.New("InternalError", "Internal Error", nil)

	_, err = s.GrantAccess(context.TODO(), id, false)
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

	// test create instance profile failure
	s = S3Repository{NamePrefix: "dataset", IAMPathPrefix: "/test/", S3: newMockS3Client(t), IAM: newMockIAMClient(t)}
	id = "78DAFEF1-E4D3-48E5-A45C-6E3CA0161F08"
	expectedCode = apierror.ErrServiceUnavailable
	expectedMessage = fmt.Sprintf("failed to create instance profile roleDataset_%s", id)
	s.IAM.(*mockIAMClient).err["CreateInstanceProfileWithContext"] = awserr.New("InternalError", "Internal Error", nil)

	_, err = s.GrantAccess(context.TODO(), id, false)
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

	// test add role to instance profile failure
	s = S3Repository{NamePrefix: "dataset", IAMPathPrefix: "/test/", S3: newMockS3Client(t), IAM: newMockIAMClient(t)}
	id = "78DAFEF1-E4D3-48E5-A45C-6E3CA0161F08"
	expectedCode = apierror.ErrServiceUnavailable
	expectedMessage = fmt.Sprintf("failed to add role to instance profile roleDataset_%s", id)
	s.IAM.(*mockIAMClient).err["AddRoleToInstanceProfileWithContext"] = awserr.New("InternalError", "Internal Error", nil)

	_, err = s.GrantAccess(context.TODO(), id, false)
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
	var expectedCode, expectedMessage, id string

	// test success
	s := S3Repository{NamePrefix: "dataset", IAMPathPrefix: "/test/", S3: newMockS3Client(t), IAM: newMockIAMClient(t)}
	id = "78DAFEF1-E4D3-48E5-A45C-6E3CA0161F08"
	err := s.RevokeAccess(context.TODO(), id)
	if err != nil {
		t.Errorf("expected nil error, got: %s", err)
	}

	// test empty id
	s = S3Repository{NamePrefix: "dataset", IAMPathPrefix: "/test/", S3: newMockS3Client(t), IAM: newMockIAMClient(t)}
	err = s.RevokeAccess(context.TODO(), "")
	if aerr, ok := err.(apierror.Error); ok {
		if aerr.Code != apierror.ErrBadRequest {
			t.Errorf("expected error code %s, got: %s", apierror.ErrBadRequest, aerr.Code)
		}
	} else {
		t.Errorf("expected apierror.Error, got: %s", reflect.TypeOf(err).String())
	}

	expectedCode = apierror.ErrInternalError
	expectedMessage = fmt.Sprintf("one or more errors trying to revoke access for data repository dataset-%s", id)

	// test list role policies failure
	s = S3Repository{NamePrefix: "dataset", IAMPathPrefix: "/test/", S3: newMockS3Client(t), IAM: newMockIAMClient(t)}
	s.IAM.(*mockIAMClient).err["ListAttachedRolePoliciesWithContext"] = awserr.New("InternalError", "Internal Error", nil)

	err = s.RevokeAccess(context.TODO(), id)
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

	// test remove role from instance profile failure
	s = S3Repository{NamePrefix: "dataset", IAMPathPrefix: "/test/", S3: newMockS3Client(t), IAM: newMockIAMClient(t)}
	s.IAM.(*mockIAMClient).err["RemoveRoleFromInstanceProfileWithContext"] = awserr.New("InternalError", "Internal Error", nil)

	err = s.RevokeAccess(context.TODO(), id)
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

	// test delete instance profile failure
	s = S3Repository{NamePrefix: "dataset", IAMPathPrefix: "/test/", S3: newMockS3Client(t), IAM: newMockIAMClient(t)}
	s.IAM.(*mockIAMClient).err["DeleteInstanceProfileWithContext"] = awserr.New("InternalError", "Internal Error", nil)

	err = s.RevokeAccess(context.TODO(), id)
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

	// test delete role failure
	s = S3Repository{NamePrefix: "dataset", IAMPathPrefix: "/test/", S3: newMockS3Client(t), IAM: newMockIAMClient(t)}
	s.IAM.(*mockIAMClient).err["DeleteRoleWithContext"] = awserr.New("InternalError", "Internal Error", nil)

	err = s.RevokeAccess(context.TODO(), id)
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
