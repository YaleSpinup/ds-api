package s3datarepository

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/YaleSpinup/ds-api/apierror"
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
	s := S3Repository{
		S3: newMockS3Client(t, nil),
	}

	// test success
	err := s.Delete(context.TODO(), "68004EEC-6044-45C9-91E5-AF836DCD9234")
	if err != nil {
		t.Errorf("expected nil error, got: %s", err)
	}

	// test empty id
	err = s.Delete(context.TODO(), "")
	if aerr, ok := err.(apierror.Error); ok {
		if aerr.Code != apierror.ErrBadRequest {
			t.Errorf("expected error code %s, got: %s", apierror.ErrBadRequest, aerr.Code)
		}
	} else {
		t.Errorf("expected apierror.Error, got: %s", reflect.TypeOf(err).String())
	}

	// test ErrCodeNoSuchBucket
	s.S3.(*mockS3Client).err = awserr.New(s3.ErrCodeNoSuchBucket, "bucket not found", nil)
	err = s.Delete(context.TODO(), "68004EEC-6044-45C9-91E5-AF836DCD9234")
	if aerr, ok := err.(apierror.Error); ok {
		if aerr.Code != apierror.ErrNotFound {
			t.Errorf("expected error code %s, got: %s", apierror.ErrNotFound, aerr.Code)
		}
	} else {
		t.Errorf("expected apierror.Error, got: %s", reflect.TypeOf(err).String())
	}

	// test NotFound
	s.S3.(*mockS3Client).err = awserr.New("NotFound", "bucket not found", nil)
	err = s.Delete(context.TODO(), "68004EEC-6044-45C9-91E5-AF836DCD9234")
	if aerr, ok := err.(apierror.Error); ok {
		if aerr.Code != apierror.ErrNotFound {
			t.Errorf("expected error code %s, got: %s", apierror.ErrNotFound, aerr.Code)
		}
	} else {
		t.Errorf("expected apierror.Error, got: %s", reflect.TypeOf(err).String())
	}

	// test BucketNotEmpty
	s.S3.(*mockS3Client).err = awserr.New("BucketNotEmpty", "bucket not empty", nil)
	err = s.Delete(context.TODO(), "68004EEC-6044-45C9-91E5-AF836DCD9234")
	if aerr, ok := err.(apierror.Error); ok {
		if aerr.Code != apierror.ErrConflict {
			t.Errorf("expected error code %s, got: %s", apierror.ErrConflict, aerr.Code)
		}
	} else {
		t.Errorf("expected apierror.Error, got: %s", reflect.TypeOf(err).String())
	}

	// test non-aws error
	s.S3.(*mockS3Client).err = errors.New("things blowing up")
	err = s.Delete(context.TODO(), "68004EEC-6044-45C9-91E5-AF836DCD9234")
	if aerr, ok := err.(apierror.Error); ok {
		if aerr.Code != apierror.ErrInternalError {
			t.Errorf("expected error code %s, got: %s", apierror.ErrInternalError, aerr.Code)
		}
	} else {
		t.Errorf("expected apierror.Error, got: %s", reflect.TypeOf(err).String())
	}
}
