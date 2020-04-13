package s3datarepository

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/aws/aws-sdk-go/service/iam/iamiface"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/aws/aws-sdk-go/service/sts/stsiface"
)

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

func (i *mockSTSClient) GetCallerIdentityWithContext(ctx context.Context, input *sts.GetCallerIdentityInput, opts ...request.Option) (*sts.GetCallerIdentityOutput, error) {
	return &sts.GetCallerIdentityOutput{
		Account: aws.String("123456789012"),
		Arn:     aws.String("arn:aws:iam::123456789012:user/test"),
		UserId:  aws.String("test"),
	}, nil
}

func TestGrantAccess(t *testing.T) {
	t.Log("TODO")
}

func TestRevokeAccess(t *testing.T) {
	t.Log("TODO")
}
