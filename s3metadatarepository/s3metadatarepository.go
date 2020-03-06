package s3metadatarepository

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/YaleSpinup/ds-api/apierror"
	"github.com/YaleSpinup/ds-api/dataset"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	log "github.com/sirupsen/logrus"
)

// S3RepositoryOption is a function to set repository options
type S3RepositoryOption func(*S3Repository)

// S3Repository is an implementation of a metadata respository in S3
type S3Repository struct {
	S3     s3iface.S3API
	Bucket string
	Prefix string
	config *aws.Config
}

// NewDefaultRepository creates a new repository from the default config data
func NewDefaultRepository(config map[string]interface{}) (*S3Repository, error) {
	var akid, secret, token, region, endpoint, bucket, prefix string
	if v, ok := config["akid"].(string); ok {
		akid = v
	}

	if v, ok := config["secret"].(string); ok {
		secret = v
	}

	if v, ok := config["token"].(string); ok {
		token = v
	}

	if v, ok := config["region"].(string); ok {
		region = v
	}

	if v, ok := config["endpoint"].(string); ok {
		endpoint = v
	}

	if v, ok := config["bucket"].(string); ok {
		bucket = v
	}

	if v, ok := config["prefix"].(string); ok {
		prefix = v
	}

	opts := []S3RepositoryOption{
		WithStaticCredentials(akid, secret, token),
	}

	if region != "" {
		opts = append(opts, WithRegion(region))
	}

	if endpoint != "" {
		opts = append(opts, WithEndpoint(endpoint))
	}

	if bucket != "" {
		opts = append(opts, WithBucket(bucket))
	}

	if prefix != "" {
		opts = append(opts, WithPrefix(prefix))
	}

	return New(opts...)
}

// New creates an S3Repository from a list of S3RepositoryOption functions
func New(opts ...S3RepositoryOption) (*S3Repository, error) {
	log.Info("creating new s3 metadata repository provider")

	s := S3Repository{}
	s.config = aws.NewConfig()

	for _, opt := range opts {
		opt(&s)
	}

	sess := session.Must(session.NewSession(s.config))

	s.S3 = s3.New(sess)
	return &s, nil
}

// WithStaticCredentials authenticates with AWS static credentials (key, secret, token)
func WithStaticCredentials(akid, secret, token string) S3RepositoryOption {
	return func(s *S3Repository) {
		log.Debugf("setting static credentials with akid %s", akid)
		s.config.WithCredentials(credentials.NewStaticCredentials(akid, secret, token))
	}
}

// WithRegion sets the region for the S3Repository
func WithRegion(region string) S3RepositoryOption {
	return func(s *S3Repository) {
		log.Debugf("setting region %s", region)
		s.config.WithRegion(region)
	}
}

// WithEndpoint sets the endpoint for the S3Repository
func WithEndpoint(endpoint string) S3RepositoryOption {
	return func(s *S3Repository) {
		log.Debugf("setting endpoint %s", endpoint)
		s.config.WithEndpoint(endpoint)
	}
}

// WithBucket sets the bucket for the S3Repository
func WithBucket(bucket string) S3RepositoryOption {
	return func(s *S3Repository) {
		log.Debugf("setting bucket %s", bucket)
		s.Bucket = bucket
	}
}

// WithPrefix sets the bucket prefix for the S3Repository
func WithPrefix(prefix string) S3RepositoryOption {
	return func(s *S3Repository) {
		log.Debugf("setting bucket prefix %s", prefix)
		s.Prefix = prefix
	}
}

// func WithLoggingBucket(bucket string) S3RepositoryOption {
// 	return func(s *S3Repository) {
// 		s.LoggingBucket = bucket
// 	}
// }

// func WithLoggingBucketPrefix(prefix string) S3RepositoryOption {
// 	return func(s *S3Repository) {
// 		s.LoggingBucketPrefix = prefix
// 	}
// }

// Create creates a new metadata object in the repository
func (s *S3Repository) Create(ctx context.Context, account, id string, metadata *dataset.Metadata) (*dataset.Metadata, error) {
	if account == "" {
		return nil, apierror.New(apierror.ErrBadRequest, "invalid input", errors.New("empty account"))
	}

	if id == "" {
		return nil, apierror.New(apierror.ErrBadRequest, "invalid input", errors.New("empty id"))
	}

	log.Debugf("creating s3metadatarepository object in account '%s' with id '%s': %+v", account, id, metadata)

	// set the created/modified time to right now
	now := time.Now().UTC().Truncate(time.Second)
	metadata.CreatedAt = &now
	metadata.ModifiedAt = &now

	key := s.Prefix + "/" + account
	if !strings.HasSuffix(account, "/") && !strings.HasPrefix(id, "/") {
		key = key + "/"
	}
	key = key + id

	j, err := json.MarshalIndent(metadata, "", "\t")
	if err != nil {
		return nil, apierror.New(apierror.ErrBadRequest, "invalid input", err)
	}

	out, err := s.S3.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Body:        bytes.NewReader(j),
		Bucket:      aws.String(s.Bucket),
		ContentType: aws.String("application/json"),
		Key:         aws.String(key),
	})
	if err != nil {
		return nil, ErrCode("failed to put s3 metadata object "+key, err)
	}

	log.Debugf("output from s3 metadata object put: %+v", out)

	return metadata, nil
}

// Get gets a metadata object from the repository by id
func (s *S3Repository) Get(ctx context.Context, account, id string) (*dataset.Metadata, error) {
	if account == "" {
		return nil, apierror.New(apierror.ErrBadRequest, "invalid input", errors.New("empty account"))
	}

	if id == "" {
		return nil, apierror.New(apierror.ErrBadRequest, "invalid input", errors.New("empty id"))
	}

	log.Debugf("getting s3metadatarepository object from account '%s' with id: %s", account, id)

	key := s.Prefix + "/" + account
	if !strings.HasSuffix(account, "/") && !strings.HasPrefix(id, "/") {
		key = key + "/"
	}
	key = key + id

	out, err := s.S3.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, ErrCode("failed to get metadata object from s3 "+key, err)
	}

	metadata := &dataset.Metadata{}
	err = json.NewDecoder(out.Body).Decode(metadata)
	if err != nil {
		return nil, apierror.New(apierror.ErrBadRequest, "failed to decode json from s3", err)
	}

	log.Debugf("output from getting s3 metadata '%s': %+v", key, metadata)

	return metadata, nil
}

// Update updates a metadata object in the repository
func (s *S3Repository) Update(ctx context.Context, account, id string, metadata *dataset.Metadata) (*dataset.Metadata, error) {
	if account == "" {
		return nil, apierror.New(apierror.ErrBadRequest, "invalid input", errors.New("empty account"))
	}

	if id == "" {
		return nil, apierror.New(apierror.ErrBadRequest, "invalid input", errors.New("empty id"))
	}

	log.Debugf("updating s3metadatarepository object in account '%s' with id '%s': %+v", account, id, metadata)

	return metadata, nil
}

// Delete deletes a metadata object from the repository by id
func (s *S3Repository) Delete(ctx context.Context, account, id string) error {
	if account == "" {
		return apierror.New(apierror.ErrBadRequest, "invalid input", errors.New("empty account"))
	}

	if id == "" {
		return apierror.New(apierror.ErrBadRequest, "invalid input", errors.New("empty id"))
	}

	log.Debugf("deleting s3metadatarepository object in account '%s' with id: %s", account, id)

	return nil
}
