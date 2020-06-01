package s3datarepository

import (
	"context"
	"fmt"
	"mime/multipart"
	"reflect"
	"testing"
	"time"

	"github.com/YaleSpinup/ds-api/apierror"
	"github.com/YaleSpinup/ds-api/dataset"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
)

// mockS3Uploader is a fake S3 Uploader
type mockS3Uploader struct {
	s3manageriface.UploaderAPI
	t   *testing.T
	err map[string]error
}

func newMockS3Uploader(t *testing.T) *mockS3Uploader {
	return &mockS3Uploader{
		t:   t,
		err: make(map[string]error),
	}
}

func (u mockS3Uploader) UploadWithContext(ctx aws.Context, input *s3manager.UploadInput, opts ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error) {
	if err, ok := u.err["UploadWithContext"]; ok {
		return nil, err
	}
	return &s3manager.UploadOutput{}, nil
}

func (m *mockS3Client) DeleteObjectWithContext(ctx aws.Context, input *s3.DeleteObjectInput, opts ...request.Option) (*s3.DeleteObjectOutput, error) {
	if err, ok := m.err["DeleteObjectWithContext"]; ok {
		return nil, err
	}

	return &s3.DeleteObjectOutput{}, nil
}

func (m *mockS3Client) ListObjectsV2WithContext(ctx aws.Context, input *s3.ListObjectsV2Input, opts ...request.Option) (*s3.ListObjectsV2Output, error) {
	if err, ok := m.err["ListObjectsV2WithContext"]; ok {
		return nil, err
	}

	if aws.StringValue(input.Bucket) == "testBucketEmpty" {
		return &s3.ListObjectsV2Output{KeyCount: aws.Int64(int64(0))}, nil
	}

	if aws.StringValue(input.Bucket) == "testBucketNotEmpty" {
		return &s3.ListObjectsV2Output{KeyCount: aws.Int64(int64(1))}, nil
	}

	time1, _ := time.Parse(time.RFC3339, "2020-01-01T01:00:00Z")
	time2, _ := time.Parse(time.RFC3339, "2020-02-02T02:00:00Z")

	if aws.StringValue(input.Prefix) == "_attachments/" {
		contents := []*s3.Object{
			&s3.Object{
				Key:          aws.String(aws.StringValue(input.Prefix) + "test1.doc"),
				LastModified: aws.Time(time1),
				Size:         aws.Int64(10000),
			},
			&s3.Object{
				Key:          aws.String(aws.StringValue(input.Prefix) + "test2.doc"),
				LastModified: aws.Time(time2),
				Size:         aws.Int64(20000),
			},
		}
		return &s3.ListObjectsV2Output{KeyCount: aws.Int64(int64(2)), Contents: contents}, nil
	}

	return nil, awserr.New(s3.ErrCodeNoSuchKey, aws.StringValue(input.Prefix)+" not found", nil)
}

func TestCreateAttachment(t *testing.T) {
	var testAttachment multipart.File

	// test success
	s := S3Repository{S3Uploader: newMockS3Uploader(t)}
	err := s.CreateAttachment(context.TODO(), "9C7BFAC0-0070-4FC2-8849-2F94A64B6FF8", "TestAttachment.txt", testAttachment)
	if err != nil {
		t.Errorf("expected nil error, got: %s", err)
	}

	// test empty id
	s = S3Repository{S3Uploader: newMockS3Uploader(t)}
	expectedCode := apierror.ErrBadRequest
	expectedMessage := "invalid input"

	err = s.CreateAttachment(context.TODO(), "", "TestAttachment.txt", testAttachment)
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

	// test empty name
	s = S3Repository{S3Uploader: newMockS3Uploader(t)}
	expectedCode = apierror.ErrBadRequest
	expectedMessage = "invalid input"

	err = s.CreateAttachment(context.TODO(), "9C7BFAC0-0070-4FC2-8849-2F94A64B6FF8", "", testAttachment)
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

	// test s3 upload failure
	s = S3Repository{NamePrefix: "dataset", S3Uploader: newMockS3Uploader(t)}
	expectedCode = apierror.ErrServiceUnavailable
	expectedMessage = fmt.Sprintf("failed to upload attachment to s3 bucket dataset-9C7BFAC0-0070-4FC2-8849-2F94A64B6FF8")
	s.S3Uploader.(*mockS3Uploader).err["UploadWithContext"] = awserr.New("InternalError", "Internal Error", nil)

	err = s.CreateAttachment(context.TODO(), "9C7BFAC0-0070-4FC2-8849-2F94A64B6FF8", "TestAttachment.txt", testAttachment)
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

func TestDeleteAttachment(t *testing.T) {
	id := "9C7BFAC0-0070-4FC2-8849-2F94A64B6FF8"

	// test success
	s := S3Repository{NamePrefix: "dataset", S3: newMockS3Client(t)}
	err := s.DeleteAttachment(context.TODO(), id, "TestAttachment.txt")
	if err != nil {
		t.Errorf("expected nil error, got: %s", err)
	}

	// test empty id
	s = S3Repository{NamePrefix: "dataset", S3: newMockS3Client(t)}
	expectedCode := apierror.ErrBadRequest
	expectedMessage := "invalid input"

	err = s.DeleteAttachment(context.TODO(), "", "TestAttachment.txt")
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

	// test empty name
	s = S3Repository{NamePrefix: "dataset", S3: newMockS3Client(t)}
	expectedCode = apierror.ErrBadRequest
	expectedMessage = "invalid input"

	err = s.DeleteAttachment(context.TODO(), id, "")
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

	// test s3 delete object failure
	s = S3Repository{NamePrefix: "dataset", S3: newMockS3Client(t)}
	expectedCode = apierror.ErrServiceUnavailable
	expectedMessage = fmt.Sprintf("failed to delete object _attachments/TestAttachment.txt")
	s.S3.(*mockS3Client).err["DeleteObjectWithContext"] = awserr.New("InternalError", "Internal Error", nil)

	err = s.DeleteAttachment(context.TODO(), id, "TestAttachment.txt")
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

func TestListAttachments(t *testing.T) {
	s := S3Repository{NamePrefix: "dataset", S3: newMockS3Client(t)}

	time1, _ := time.Parse(time.RFC3339, "2020-01-01T01:00:00Z")
	time2, _ := time.Parse(time.RFC3339, "2020-02-02T02:00:00Z")
	expected := []dataset.Attachment{
		dataset.Attachment{
			Name:     "test1.doc",
			Modified: time1,
			Size:     10000,
		},
		dataset.Attachment{
			Name:     "test2.doc",
			Modified: time2,
			Size:     20000,
		},
	}

	// test success, don't generate presigned URL
	got, err := s.ListAttachments(context.TODO(), "9C7BFAC0-0070-4FC2-8849-2F94A64B6FF8", false)
	if err != nil {
		t.Errorf("expected nil error, got: %s", err)
	}
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("expected output:\n%+v, got:\n%+v", expected, got)
	}

	// test empty id
	s = S3Repository{NamePrefix: "dataset", S3: newMockS3Client(t)}
	expectedCode := apierror.ErrBadRequest
	expectedMessage := "invalid input"

	_, err = s.ListAttachments(context.TODO(), "", false)
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

	// test s3 list object failure
	s = S3Repository{NamePrefix: "dataset", S3: newMockS3Client(t)}
	expectedCode = apierror.ErrServiceUnavailable
	expectedMessage = fmt.Sprintf("failed to list objects from s3")
	s.S3.(*mockS3Client).err["ListObjectsV2WithContext"] = awserr.New("InternalError", "Internal Error", nil)

	_, err = s.ListAttachments(context.TODO(), "9C7BFAC0-0070-4FC2-8849-2F94A64B6FF8", false)
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
