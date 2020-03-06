package s3datarepository

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/YaleSpinup/ds-api/apierror"
	"github.com/YaleSpinup/ds-api/dataset"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

// mockS3Client is a fake S3 client
type mockS3Client struct {
	s3iface.S3API
	t         *testing.T
	err       map[string]error
	headCount uint
}

func newMockS3Client(t *testing.T) s3iface.S3API {
	return &mockS3Client{
		t:         t,
		err:       make(map[string]error),
		headCount: 0,
	}
}

func (m *mockS3Client) HeadBucketWithContext(ctx context.Context, input *s3.HeadBucketInput, opts ...request.Option) (*s3.HeadBucketOutput, error) {
	if err, ok := m.err["HeadBucketWithContext"]; ok {
		if m.headCount == 0 {
			m.headCount++
			return nil, err
		}
	}

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
	if err, ok := m.err["CreateBucketWithContext"]; ok {
		return nil, err
	}
	return &s3.CreateBucketOutput{Location: aws.String("/testbucket")}, nil
}

func (m *mockS3Client) DeleteBucketWithContext(ctx context.Context, input *s3.DeleteBucketInput, opts ...request.Option) (*s3.DeleteBucketOutput, error) {
	if err, ok := m.err["DeleteBucketWithContext"]; ok {
		return nil, err
	}
	return &s3.DeleteBucketOutput{}, nil
}

func (m *mockS3Client) PutBucketEncryptionWithContext(ctx context.Context, input *s3.PutBucketEncryptionInput, opts ...request.Option) (*s3.PutBucketEncryptionOutput, error) {
	if err, ok := m.err["PutBucketEncryptionWithContext"]; ok {
		return nil, err
	}
	return &s3.PutBucketEncryptionOutput{}, nil
}

func (m *mockS3Client) PutBucketTaggingWithContext(ctx context.Context, input *s3.PutBucketTaggingInput, opts ...request.Option) (*s3.PutBucketTaggingOutput, error) {
	if err, ok := m.err["PutBucketTaggingWithContext"]; ok {
		return nil, err
	}
	return &s3.PutBucketTaggingOutput{}, nil
}

func (m *mockS3Client) PutPublicAccessBlockWithContext(ctx context.Context, input *s3.PutPublicAccessBlockInput, opts ...request.Option) (*s3.PutPublicAccessBlockOutput, error) {
	if err, ok := m.err["PutPublicAccessBlockWithContext"]; ok {
		return nil, err
	}
	return &s3.PutPublicAccessBlockOutput{}, nil
}

func TestNewDefaultRepository(t *testing.T) {
	testConfig := map[string]interface{}{
		"region":   "us-east-1",
		"akid":     "xxxxx",
		"secret":   "yyyyy",
		"endpoint": "https://under.mydesk.amazonaws.com",
	}

	s, err := NewDefaultRepository(testConfig)
	if err != nil {
		t.Errorf("expected nil error, got: %s", err)
	}
	to := reflect.TypeOf(s).String()
	if to != "*s3datarepository.S3Repository" {
		t.Errorf("expected type to be '*s3datarepository.S3Repository', got %s", to)
	}

	if s.config.Credentials == nil {
		t.Error("expected config Credentials to be set, got nil")
	}

	if s.config.Region == nil {
		t.Error("expected config Region to be set, got nil")
	}

	if s.config.Endpoint == nil {
		t.Error("expected config Endpoint to be set, got nil")
	}
}

func TestNew(t *testing.T) {
	s, err := New()
	if err != nil {
		t.Errorf("expected nil error, got: %s", err)
	}
	to := reflect.TypeOf(s).String()
	if to != "*s3datarepository.S3Repository" {
		t.Errorf("expected type to be '*s3datarepository.S3Repository', got %s", to)
	}
}

func TestBucketExists(t *testing.T) {
	s := S3Repository{
		S3: newMockS3Client(t),
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
	var expectedCode, expectedMessage, id string

	testTags := []*dataset.Tag{
		&dataset.Tag{
			Key:   aws.String("ID"),
			Value: aws.String("68004EEC-6044-45C9-91E5-AF836DCD9234"),
		},
		&dataset.Tag{
			Key:   aws.String("Name"),
			Value: aws.String("dataset"),
		},
	}

	// test success, with tags
	s := S3Repository{S3: newMockS3Client(t)}
	s.S3.(*mockS3Client).err["HeadBucketWithContext"] = awserr.New("NotFound", "bucket not found", nil)

	err := s.Provision(context.TODO(), "68004EEC-6044-45C9-91E5-AF836DCD9234", testTags)
	if err != nil {
		t.Errorf("expected nil error, got: %s", err)
	}

	// test success, without tags
	s = S3Repository{NamePrefix: "dataset", S3: newMockS3Client(t)}
	s.S3.(*mockS3Client).err["HeadBucketWithContext"] = awserr.New("NotFound", "bucket not found", nil)

	err = s.Provision(context.TODO(), "68004EEC-6044-45C9-91E5-AF836DCD9234", []*dataset.Tag{})
	if err != nil {
		t.Errorf("expected nil error, got: %s", err)
	}

	// test empty id
	s = S3Repository{S3: newMockS3Client(t)}
	id = ""
	expectedCode = apierror.ErrBadRequest
	expectedMessage = "invalid input"

	err = s.Provision(context.TODO(), id, testTags)
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

	// test existing id
	s = S3Repository{S3: newMockS3Client(t)}
	id = "68004EEC-6044-45C9-91E5-AF836DCD9234-exists"
	expectedCode = apierror.ErrConflict
	expectedMessage = "s3 bucket already exists"

	err = s.Provision(context.TODO(), id, testTags)
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

	// test bucket create failure
	s = S3Repository{NamePrefix: "dataset", S3: newMockS3Client(t)}
	id = "68004EEC-6044-45C9-91E5-AF836DCD9234"
	expectedCode = apierror.ErrServiceUnavailable
	expectedMessage = fmt.Sprintf("failed to create s3 bucket dataset-%s", id)
	s.S3.(*mockS3Client).err["HeadBucketWithContext"] = awserr.New("NotFound", "bucket not found", nil)
	s.S3.(*mockS3Client).err["CreateBucketWithContext"] = awserr.New("InternalError", "Internal Error", nil)

	err = s.Provision(context.TODO(), id, testTags)
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

	// test bucket create timeout failure
	s = S3Repository{NamePrefix: "dataset", S3: newMockS3Client(t)}
	id = "68004EEC-6044-45C9-91E5-AF836DCD9234-missing"
	expectedCode = apierror.ErrInternalError
	expectedMessage = fmt.Sprintf("failed to create bucket dataset-%s, timeout waiting for create: s3 bucket (dataset-%s) doesn't exist", id, id)

	err = s.Provision(context.TODO(), id, testTags)
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

	// test bucket block public access failure
	s = S3Repository{NamePrefix: "dataset", S3: newMockS3Client(t)}
	id = "68004EEC-6044-45C9-91E5-AF836DCD9234"
	expectedCode = apierror.ErrServiceUnavailable
	expectedMessage = fmt.Sprintf("failed block public access for s3 bucket dataset-%s", id)
	s.S3.(*mockS3Client).err["HeadBucketWithContext"] = awserr.New("NotFound", "bucket not found", nil)
	s.S3.(*mockS3Client).err["PutPublicAccessBlockWithContext"] = awserr.New("InternalError", "Internal Error", nil)

	err = s.Provision(context.TODO(), id, testTags)
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

	// test bucket enable encryption failure
	s = S3Repository{NamePrefix: "dataset", S3: newMockS3Client(t)}
	id = "68004EEC-6044-45C9-91E5-AF836DCD9234"
	expectedCode = apierror.ErrServiceUnavailable
	expectedMessage = fmt.Sprintf("failed to enable encryption for s3 bucket dataset-%s", id)
	s.S3.(*mockS3Client).err["HeadBucketWithContext"] = awserr.New("NotFound", "bucket not found", nil)
	s.S3.(*mockS3Client).err["PutBucketEncryptionWithContext"] = awserr.New("InternalError", "Internal Error", nil)

	err = s.Provision(context.TODO(), id, testTags)
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

	// test bucket tagging failure
	s = S3Repository{NamePrefix: "dataset", S3: newMockS3Client(t)}
	id = "68004EEC-6044-45C9-91E5-AF836DCD9234"
	expectedCode = apierror.ErrServiceUnavailable
	expectedMessage = fmt.Sprintf("failed to tag s3 bucket dataset-%s", id)
	s.S3.(*mockS3Client).err["HeadBucketWithContext"] = awserr.New("NotFound", "bucket not found", nil)
	s.S3.(*mockS3Client).err["PutBucketTaggingWithContext"] = awserr.New("InternalError", "Internal Error", nil)

	err = s.Provision(context.TODO(), id, testTags)
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

func TestDeprovision(t *testing.T) {
	t.Log("TODO")
}

func TestDelete(t *testing.T) {
	var expectedCode, expectedMessage, id string

	// test success
	s := S3Repository{S3: newMockS3Client(t)}
	err := s.Delete(context.TODO(), "68004EEC-6044-45C9-91E5-AF836DCD9234")
	if err != nil {
		t.Errorf("expected nil error, got: %s", err)
	}

	// test empty id
	s = S3Repository{S3: newMockS3Client(t)}
	err = s.Delete(context.TODO(), "")
	if aerr, ok := err.(apierror.Error); ok {
		if aerr.Code != apierror.ErrBadRequest {
			t.Errorf("expected error code %s, got: %s", apierror.ErrBadRequest, aerr.Code)
		}
	} else {
		t.Errorf("expected apierror.Error, got: %s", reflect.TypeOf(err).String())
	}

	// test ErrCodeNoSuchBucket
	s = S3Repository{NamePrefix: "dataset", S3: newMockS3Client(t)}
	id = "68004EEC-6044-45C9-91E5-AF836DCD9234"
	expectedCode = apierror.ErrNotFound
	expectedMessage = fmt.Sprintf("failed to delete s3 bucket dataset-%s", id)
	s.S3.(*mockS3Client).err["DeleteBucketWithContext"] = awserr.New(s3.ErrCodeNoSuchBucket, "bucket not found", nil)

	err = s.Delete(context.TODO(), id)
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

	// test NotFound
	s = S3Repository{NamePrefix: "dataset", S3: newMockS3Client(t)}
	id = "68004EEC-6044-45C9-91E5-AF836DCD9234"
	expectedCode = apierror.ErrNotFound
	expectedMessage = fmt.Sprintf("failed to delete s3 bucket dataset-%s", id)
	s.S3.(*mockS3Client).err["DeleteBucketWithContext"] = awserr.New("NotFound", "bucket not found", nil)

	err = s.Delete(context.TODO(), id)
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

	// test BucketNotEmpty
	s = S3Repository{NamePrefix: "dataset", S3: newMockS3Client(t)}
	id = "68004EEC-6044-45C9-91E5-AF836DCD9234"
	expectedCode = apierror.ErrConflict
	expectedMessage = fmt.Sprintf("failed to delete s3 bucket dataset-%s", id)
	s.S3.(*mockS3Client).err["DeleteBucketWithContext"] = awserr.New("BucketNotEmpty", "bucket not empty", nil)

	err = s.Delete(context.TODO(), id)
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

	// test non-aws error
	s = S3Repository{NamePrefix: "dataset", S3: newMockS3Client(t)}
	id = "68004EEC-6044-45C9-91E5-AF836DCD9234"
	expectedCode = apierror.ErrInternalError
	expectedMessage = fmt.Sprintf("failed to delete s3 bucket dataset-%s", id)
	s.S3.(*mockS3Client).err["DeleteBucketWithContext"] = errors.New("things blowing up")

	err = s.Delete(context.TODO(), id)
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
