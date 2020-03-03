package s3datarepository

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

var testTime = time.Now().UTC().Truncate(time.Second)

// mockS3Client is a fake S3 client
type mockS3Client struct {
	s3iface.S3API
	t   *testing.T
	err error
}

func newMockS3Client(t *testing.T, err error) s3iface.S3API {
	return &mockS3Client{
		t:   t,
		err: err,
	}
}

func (m *mockS3Client) HeadBucketWithContext(ctx context.Context, input *s3.HeadBucketInput, opts ...request.Option) (*s3.HeadBucketOutput, error) {
	if aws.StringValue(input.Bucket) == "testbucket" {
		return nil, awserr.New(s3.ErrCodeNoSuchBucket, "Not Found", nil)
	}

	if strings.HasSuffix(aws.StringValue(input.Bucket), "-exists") {
		return &s3.HeadBucketOutput{}, nil
	}

	if strings.HasSuffix(aws.StringValue(input.Bucket), "-missing") {
		return nil, awserr.New(s3.ErrCodeNoSuchBucket, "Not Found", nil)
	}

	return &s3.HeadBucketOutput{}, nil
}

func (m *mockS3Client) CreateBucketWithContext(ctx context.Context, input *s3.CreateBucketInput, opts ...request.Option) (*s3.CreateBucketOutput, error) {
	if m.err != nil {
		return nil, m.err
	}
	return &s3.CreateBucketOutput{Location: aws.String("/testbucket")}, nil
}

func (m *mockS3Client) DeleteBucketWithContext(ctx context.Context, input *s3.DeleteBucketInput, opts ...request.Option) (*s3.DeleteBucketOutput, error) {
	if m.err != nil {
		return nil, m.err
	}
	return &s3.DeleteBucketOutput{}, nil
}

func TestWithStaticCredentials(t *testing.T) {
	t.Log("TODO")
}

func TestWithRegion(t *testing.T) {
	t.Log("TODO")
}

func TestWithEndpoint(t *testing.T) {
	t.Log("TODO")
}

func TestWithBucket(t *testing.T) {
	t.Log("TODO")
}

func TestWithPrefix(t *testing.T) {
	t.Log("TODO")
}

func TestBucketExists(t *testing.T) {
	s := S3Repository{
		S3: newMockS3Client(t, nil),
	}

	exists, err := s.bucketExists(context.TODO(), "testbucket-exists")
	if err != nil {
		t.Errorf("expected nil error, got: %s", err)
	}

	if !exists {
		t.Errorf("expected testbucket-exists to exist (true), got false")
	}

	notexists, err := s.bucketExists(context.TODO(), "testbucket-missing")
	if err != nil {
		t.Errorf("expected nil error, got: %s", err)
	}

	if notexists {
		t.Errorf("expected testbucket-missing to not exist (false), got true")
	}
}

func TestProvision(t *testing.T) {
	t.Log("TODO")
}

func TestDeprovision(t *testing.T) {
	t.Log("TODO")
}

func TestDelete(t *testing.T) {
	t.Log("TODO")
}
