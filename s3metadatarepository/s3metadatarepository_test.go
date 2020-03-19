package s3metadatarepository

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/YaleSpinup/ds-api/apierror"
	"github.com/YaleSpinup/ds-api/dataset"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

var testTime = time.Now().UTC().Truncate(time.Second)

var testMetadata = map[string]dataset.Metadata{
	"2D24607A-38DD-4E11-8A83-5F317ADA24F1": dataset.Metadata{
		ID:                  "2D24607A-38DD-4E11-8A83-5F317ADA24F1",
		Name:                "huge-awesome-dataset",
		Description:         "The hugest dataset of awesome stuff",
		CreatedAt:           &testTime,
		CreatedBy:           "Good Guy",
		DataClassifications: []string{"HIPAA", "PHI"},
		DataFormat:          "file",
		DataStorage:         "s3",
		Derivative:          false,
		DuaURL:              &url.URL{Scheme: "https", Host: "allmydata.s3.amazonaws.com", Path: "/duas/huge_awesome_dua.pdf"},
		ModifiedAt:          &testTime,
		ModifiedBy:          "Bad Guy",
		ProctorResponseURL:  &url.URL{Scheme: "https", Host: "allmydata.s3.amazonaws.com", Path: "/proctor/huge_awesome_study.json"},
		SourceIDs:           []string{"e15d2282-9c68-46b5-801c-2b5a62484624", "a7c082ee-f711-48fa-8a57-25c95b3a6ddd"},
	},
	"8B7842E1-9032-4C8B-942E-B58FBA8E5744": dataset.Metadata{
		ID:                  "8B7842E1-9032-4C8B-942E-B58FBA8E5744",
		Name:                "teeny-tiny-dataset",
		Description:         "The tiniest dataset of mediocre stuff",
		CreatedAt:           &testTime,
		CreatedBy:           "Yo Daddy",
		DataClassifications: []string{},
		DataFormat:          "file",
		DataStorage:         "s3",
		Derivative:          true,
		DuaURL:              &url.URL{},
		ModifiedAt:          &testTime,
		ModifiedBy:          "Yo Mama",
		ProctorResponseURL:  &url.URL{Scheme: "https", Host: "allmydata.s3.amazonaws.com", Path: "/proctor/huge_awesome_study.json"},
		SourceIDs:           []string{},
	},
}

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

func (m *mockS3Client) DeleteObjectWithContext(ctx aws.Context, input *s3.DeleteObjectInput, opts ...request.Option) (*s3.DeleteObjectOutput, error) {
	if err, ok := m.err["DeleteObjectWithContext"]; ok {
		return nil, err
	}
	return &s3.DeleteObjectOutput{}, nil
}

func (m *mockS3Client) GetObjectWithContext(ctx aws.Context, input *s3.GetObjectInput, opts ...request.Option) (*s3.GetObjectOutput, error) {
	if err, ok := m.err["GetObjectWithContext"]; ok {
		return nil, err
	}

	for k, v := range testMetadata {
		if strings.HasSuffix(aws.StringValue(input.Key), k) {
			out, err := json.Marshal(v)
			if err != nil {
				return nil, awserr.New("Internal Server Error", "failed marshalling json", err)
			}
			return &s3.GetObjectOutput{Body: ioutil.NopCloser(bytes.NewReader(out))}, nil
		}
	}

	return nil, awserr.New(s3.ErrCodeNoSuchKey, aws.StringValue(input.Key)+" not found", nil)
}

func (m *mockS3Client) PutObjectWithContext(ctx aws.Context, input *s3.PutObjectInput, opts ...request.Option) (*s3.PutObjectOutput, error) {
	if err, ok := m.err["PutObjectWithContext"]; ok {
		return nil, err
	}
	return &s3.PutObjectOutput{}, nil
}

func TestNewDefaultRepository(t *testing.T) {
	testConfig := map[string]interface{}{
		"region":   "us-east-1",
		"akid":     "xxxxx",
		"secret":   "yyyyy",
		"bucket":   "somethingspecial",
		"prefix":   "slash",
		"endpoint": "https://under.mydesk.amazonaws.com",
	}

	s, err := NewDefaultRepository(testConfig)
	if err != nil {
		t.Errorf("expected nil error, got: %s", err)
	}
	to := reflect.TypeOf(s).String()
	if to != "*s3metadatarepository.S3Repository" {
		t.Errorf("expected type to be '*s3metadatarepository.S3Repository', got %s", to)
	}

	if s.Bucket == "" {
		t.Error("expected Bucket to be 'somethingspecial', got ''")
	}

	if s.Prefix == "" {
		t.Error("expected Prefix to be 'somethingspecial', got ''")
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
	if to != "*s3metadatarepository.S3Repository" {
		t.Errorf("expected type to be '*s3metadatarepository.S3Repository', got %s", to)
	}
}

func TestCreate(t *testing.T) {
	var expectedCode, expectedMessage, id string

	testBucket := "test-bucket"
	testPrefix := "slash"
	testMetadata := &dataset.Metadata{
		ID:                  "2D24607A-38DD-4E11-8A83-5F317ADA24F1",
		Name:                "huge-awesome-dataset",
		Description:         "The hugest dataset of awesome stuff",
		CreatedBy:           "Good Guy",
		DataClassifications: []string{"HIPAA", "PHI"},
		DataFormat:          "file",
		DataStorage:         "s3",
		Derivative:          false,
		DuaURL:              &url.URL{Scheme: "https", Host: "allmydata.s3.amazonaws.com", Path: "/duas/huge_awesome_dua.pdf"},
		ModifiedBy:          "Bad Guy",
		ProctorResponseURL:  &url.URL{Scheme: "https", Host: "allmydata.s3.amazonaws.com", Path: "/proctor/huge_awesome_study.json"},
		SourceIDs:           []string{"e15d2282-9c68-46b5-801c-2b5a62484624", "a7c082ee-f711-48fa-8a57-25c95b3a6ddd"},
	}

	s := S3Repository{
		S3:     newMockS3Client(t),
		Bucket: testBucket,
		Prefix: testPrefix,
	}

	account := "burn"

	// test success
	id = "2D24607A-38DD-4E11-8A83-5F317ADA24F1"
	need := &dataset.Metadata{
		ID:                  "2D24607A-38DD-4E11-8A83-5F317ADA24F1",
		Name:                "huge-awesome-dataset",
		Description:         "The hugest dataset of awesome stuff",
		CreatedAt:           &testTime,
		CreatedBy:           "Good Guy",
		DataClassifications: []string{"HIPAA", "PHI"},
		DataFormat:          "file",
		DataStorage:         "s3",
		Derivative:          false,
		DuaURL:              &url.URL{Scheme: "https", Host: "allmydata.s3.amazonaws.com", Path: "/duas/huge_awesome_dua.pdf"},
		ModifiedAt:          &testTime,
		ModifiedBy:          "Bad Guy",
		ProctorResponseURL:  &url.URL{Scheme: "https", Host: "allmydata.s3.amazonaws.com", Path: "/proctor/huge_awesome_study.json"},
		SourceIDs:           []string{"e15d2282-9c68-46b5-801c-2b5a62484624", "a7c082ee-f711-48fa-8a57-25c95b3a6ddd"},
	}

	got, err := s.Create(context.TODO(), account, id, testMetadata)
	if err != nil {
		t.Errorf("expected nil error, got: %s", err)
	}
	if !reflect.DeepEqual(need, got) {
		t.Errorf("expected: %+v, got: %+v", need, got)
	}

	// test empty account
	expectedCode = apierror.ErrBadRequest
	expectedMessage = "invalid input"

	_, err = s.Create(context.TODO(), "", id, testMetadata)
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

	// test empty id
	id = ""
	expectedCode = apierror.ErrBadRequest
	expectedMessage = "invalid input"

	_, err = s.Create(context.TODO(), account, id, testMetadata)
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

	// test object create failure
	id = "2D24607A-38DD-4E11-8A83-5F317ADA24F1"
	expectedCode = apierror.ErrServiceUnavailable
	expectedMessage = fmt.Sprintf("failed to put s3 metadata object: %s/%s/%s", testPrefix, account, id)
	s.S3.(*mockS3Client).err["PutObjectWithContext"] = awserr.New("InternalError", "Internal Error", nil)

	_, err = s.Create(context.TODO(), account, id, testMetadata)
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

func TestGet(t *testing.T) {
	testBucket := "test-bucket"
	testPrefix := "slash"

	s := S3Repository{
		S3:     newMockS3Client(t),
		Bucket: testBucket,
		Prefix: testPrefix,
	}

	account := "burn"

	// test empty account
	_, err := s.Get(context.TODO(), "", "123456")
	if aerr, ok := err.(apierror.Error); ok {
		if aerr.Code != apierror.ErrBadRequest {
			t.Errorf("expected error code %s, got: %s", apierror.ErrBadRequest, aerr.Code)
		}
	} else {
		t.Errorf("expected apierror.Error, got: %s", reflect.TypeOf(err).String())
	}

	// test empty id
	_, err = s.Get(context.TODO(), account, "")
	if aerr, ok := err.(apierror.Error); ok {
		if aerr.Code != apierror.ErrBadRequest {
			t.Errorf("expected error code %s, got: %s", apierror.ErrBadRequest, aerr.Code)
		}
	} else {
		t.Errorf("expected apierror.Error, got: %s", reflect.TypeOf(err).String())
	}

	for k, v := range testMetadata {
		need := &dataset.Metadata{
			ID:                  v.ID,
			Name:                v.Name,
			Description:         v.Description,
			CreatedAt:           v.CreatedAt,
			CreatedBy:           v.CreatedBy,
			DataClassifications: v.DataClassifications,
			DataFormat:          v.DataFormat,
			DataStorage:         v.DataStorage,
			Derivative:          v.Derivative,
			DuaURL:              v.DuaURL,
			ModifiedAt:          v.ModifiedAt,
			ModifiedBy:          v.ModifiedBy,
			ProctorResponseURL:  v.ProctorResponseURL,
			SourceIDs:           v.SourceIDs,
		}

		got, err := s.Get(context.TODO(), account, k)
		if err != nil {
			t.Errorf("expected nil error, got %s", err)
		}

		if !reflect.DeepEqual(need, got) {
			t.Errorf("expected: %+v, got: %+v", need, got)
		}
	}

	// test getting non-existing object
	id := "are-you-there"
	expectedCode := apierror.ErrNotFound
	expectedMessage := fmt.Sprintf("failed to get metadata object from s3: %s/%s/%s", testPrefix, account, id)

	_, err = s.Get(context.TODO(), account, id)
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

func TestUpdate(t *testing.T) {
	t.Log("TODO")
}

func TestDelete(t *testing.T) {
	testBucket := "test-bucket"
	testPrefix := "slash"

	s := S3Repository{
		S3:     newMockS3Client(t),
		Bucket: testBucket,
		Prefix: testPrefix,
	}

	account := "burn"
	id := "FB3B3E9F-36EE-4920-ADE0-2D54B80FE73C"

	// test empty account
	err := s.Delete(context.TODO(), "", id)
	if aerr, ok := err.(apierror.Error); ok {
		if aerr.Code != apierror.ErrBadRequest {
			t.Errorf("expected error code %s, got: %s", apierror.ErrBadRequest, aerr.Code)
		}
	} else {
		t.Errorf("expected apierror.Error, got: %s", reflect.TypeOf(err).String())
	}

	// test empty id
	err = s.Delete(context.TODO(), account, "")
	if aerr, ok := err.(apierror.Error); ok {
		if aerr.Code != apierror.ErrBadRequest {
			t.Errorf("expected error code %s, got: %s", apierror.ErrBadRequest, aerr.Code)
		}
	} else {
		t.Errorf("expected apierror.Error, got: %s", reflect.TypeOf(err).String())
	}

	// test success
	err = s.Delete(context.TODO(), account, id)
	if err != nil {
		t.Errorf("expected nil error, got %s", err)
	}

	// test deleting non-existing object
	expectedCode := apierror.ErrNotFound
	expectedMessage := fmt.Sprintf("failed to delete s3 metadata object: %s/%s/%s", testPrefix, account, id)
	s.S3.(*mockS3Client).err["DeleteObjectWithContext"] = awserr.New("NotFound", "object not found", nil)

	err = s.Delete(context.TODO(), account, id)
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
