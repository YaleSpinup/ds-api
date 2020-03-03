package s3datarepository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/YaleSpinup/ds-api/apierror"
	"github.com/YaleSpinup/ds-api/dataset"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	log "github.com/sirupsen/logrus"
)

// S3RepositoryOption is a function to set repository options
type S3RepositoryOption func(*S3Repository)

// S3Repository is an implementation of a data respository in S3
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

// bucketExists checks if a bucket exists and we have access to it
func (s *S3Repository) bucketExists(ctx context.Context, bucketName string) (bool, error) {
	if _, err := s.S3.HeadBucketWithContext(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	}); err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchBucket, "NotFound":
				return false, nil
			case "Forbidden":
				msg := fmt.Sprintf("forbidden to access requested bucket %s: %s", bucketName, aerr.Error())
				return true, apierror.New(apierror.ErrForbidden, msg, err)
			default:
				return false, apierror.New(apierror.ErrBadRequest, aerr.Message(), err)
			}
		}
		return false, apierror.New(apierror.ErrInternalError, "unexpected error checking for bucket", err)
	}

	return true, nil
}

// Provision creates and configures a data repository in S3, and creates a default IAM policy
// 1. Check if the requested bucket already exists in S3
// 2. Create the bucket and wait for it to be successfully created
// 3. Block all public access to the bucket
// 4. Enable AWS managed serverside encryption (AES-256) for the bucket
// 5. Add tags to the bucket
func (s *S3Repository) Provision(ctx context.Context, id string, rawtags []*dataset.Tag) error {
	if id == "" {
		return apierror.New(apierror.ErrBadRequest, "invalid input", errors.New("empty id"))
	}

	log.Debugf("provisioning s3datarepository with id: %s", id)

	name := "dataset-" + id

	// checks if a bucket exists in the account
	// in us-east-1 (only) bucket creation will succeed if the bucket already exists in your
	// account, but in all other regions the API will return s3.ErrCodeBucketAlreadyOwnedByYou 🤷‍♂️
	if exists, err := s.bucketExists(ctx, name); exists {
		return apierror.New(apierror.ErrConflict, "s3 bucket already exists", nil)
	} else if err != nil {
		return apierror.New(apierror.ErrInternalError, "internal error", nil)
	}

	// prepare tags
	tags := make([]*s3.Tag, len(rawtags))
	for i, tag := range rawtags {
		tags[i] = &s3.Tag{
			Key:   tag.Key,
			Value: tag.Value,
		}
	}

	// setup rollback function list and defer execution
	var rollBackTasks []func() error
	var err error
	defer func() {
		if err != nil {
			log.Errorf("recovering from error provisioning s3datarepository: %s, executing %d rollback tasks", err, len(rollBackTasks))
			rollBack(&rollBackTasks)
		}
	}()

	// create s3 bucket
	log.Debugf("creating s3 bucket: %s", name)
	if _, err = s.S3.CreateBucketWithContext(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(name),
	}); err != nil {
		return ErrCode("failed to create s3 bucket "+name, err)
	}

	// append bucket delete to rollback tasks
	rbfunc := func() error {
		return func() error {
			log.Debugf("deleting s3 bucket: %s", name)
			if _, err := s.S3.DeleteBucketWithContext(ctx, &s3.DeleteBucketInput{Bucket: aws.String(name)}); err != nil {
				return err
			}
			return nil
		}()
	}
	rollBackTasks = append(rollBackTasks, rbfunc)

	// wait for bucket to exist
	err = retry(3, 2*time.Second, func() error {
		log.Debugf("checking if s3 bucket is created before continuing: %s", name)
		exists, err := s.bucketExists(ctx, name)
		if err != nil {
			return err
		}

		if exists {
			log.Debugf("s3 bucket %s created successfully", name)
			return nil
		}

		msg := fmt.Sprintf("s3 bucket (%s) doesn't exist", name)
		return errors.New(msg)
	})

	if err != nil {
		msg := fmt.Sprintf("failed to create bucket %s, timeout waiting for create: %s", name, err.Error())
		return apierror.New(apierror.ErrInternalError, msg, err)
	}

	// block public access
	log.Debugf("blocking all public access for bucket: %s", name)
	if _, err = s.S3.PutPublicAccessBlockWithContext(ctx, &s3.PutPublicAccessBlockInput{
		Bucket: aws.String(name),
		PublicAccessBlockConfiguration: &s3.PublicAccessBlockConfiguration{
			BlockPublicAcls:       aws.Bool(true),
			BlockPublicPolicy:     aws.Bool(true),
			IgnorePublicAcls:      aws.Bool(true),
			RestrictPublicBuckets: aws.Bool(true),
		},
	}); err != nil {
		return ErrCode("failed block public access for s3 bucket "+name, err)
	}

	// enable AWS managed serverside encryption for the bucket
	log.Debugf("enabling s3 encryption for bucket: %s", name)
	if _, err = s.S3.PutBucketEncryptionWithContext(ctx, &s3.PutBucketEncryptionInput{
		Bucket: aws.String(name),
		ServerSideEncryptionConfiguration: &s3.ServerSideEncryptionConfiguration{
			Rules: []*s3.ServerSideEncryptionRule{
				&s3.ServerSideEncryptionRule{
					ApplyServerSideEncryptionByDefault: &s3.ServerSideEncryptionByDefault{
						SSEAlgorithm: aws.String("AES256"),
					},
				},
			},
		},
	}); err != nil {
		return ErrCode("failed to enable encryption for s3 bucket "+name, err)
	}

	// add tags
	if len(tags) > 0 {
		log.Debugf("adding tags for bucket: %s\n%+v", name, tags)
		if _, err = s.S3.PutBucketTaggingWithContext(ctx, &s3.PutBucketTaggingInput{
			Bucket:  aws.String(name),
			Tagging: &s3.Tagging{TagSet: tags},
		}); err != nil {
			return ErrCode("failed to tag s3 bucket "+name, err)
		}
	}

	// TODO: create IAM policy

	return nil
}

// Deprovision satisfies the ability to deprovision a data repository
func (s *S3Repository) Deprovision(ctx context.Context, id string) error {
	if id == "" {
		return apierror.New(apierror.ErrBadRequest, "invalid input", errors.New("empty id"))
	}

	log.Debugf("deprovisioning s3datarepository with id: %s", id)

	return nil
}

// Delete deletes a data repository in S3
func (s *S3Repository) Delete(ctx context.Context, id string) error {
	if id == "" {
		return apierror.New(apierror.ErrBadRequest, "invalid input", errors.New("empty id"))
	}

	log.Debugf("deleting s3datarepository with id: %s", id)

	name := "dataset-" + id

	// delete the s3 bucket
	_, err := s.S3.DeleteBucketWithContext(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(name),
	})
	if err != nil {
		return ErrCode("failed to delete s3 bucket", err)
	}

	return nil
}
