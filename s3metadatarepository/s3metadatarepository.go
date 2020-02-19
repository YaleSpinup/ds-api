package s3metadatarepository

import (
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
	log.Info("creating new s3 repository provider")

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

// Read satisfies the ability to read metadata from the repository by id
func (s *S3Repository) Read(id string) ([]byte, error) {
	return []byte{}, nil
}

// Save satisfies the ability to save metadata to the repository
func (s *S3Repository) Save(id string, data []byte) error {
	return nil
}

// Delete satisfies the ability to delete metadata from the repository by id
func (s *S3Repository) Delete(id string) error {
	return nil
}
