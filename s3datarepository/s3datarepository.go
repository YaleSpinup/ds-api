package s3datarepository

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/YaleSpinup/ds-api/apierror"
	"github.com/YaleSpinup/ds-api/dataset"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/ec2/ec2iface"
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/aws/aws-sdk-go/service/iam/iamiface"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/aws/aws-sdk-go/service/sts/stsiface"
	log "github.com/sirupsen/logrus"
)

// S3RepositoryOption is a function to set repository options
type S3RepositoryOption func(*S3Repository)

// S3Repository is an implementation of a data respository in S3
type S3Repository struct {
	NamePrefix    string
	IAMPathPrefix string
	EC2           ec2iface.EC2API
	IAM           iamiface.IAMAPI
	S3            s3iface.S3API
	S3Uploader    s3manageriface.UploaderAPI
	STS           stsiface.STSAPI
	config        *aws.Config
}

// NewDefaultRepository creates a new repository from the default config data
func NewDefaultRepository(config map[string]interface{}) (*S3Repository, error) {
	var akid, secret, token, region, endpoint string
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

	opts := []S3RepositoryOption{
		WithStaticCredentials(akid, secret, token),
	}

	if region != "" {
		opts = append(opts, WithRegion(region))
	}

	if endpoint != "" {
		opts = append(opts, WithEndpoint(endpoint))
	}

	// set default IAMPathPrefix
	opts = append(opts, WithIAMPathPrefix("/spinup/dataset/"))

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

	s.EC2 = ec2.New(sess)
	s.IAM = iam.New(sess)
	s.S3 = s3.New(sess)
	s.S3Uploader = s3manager.NewUploaderWithClient(s.S3)
	s.STS = sts.New(sess)

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

// WithIAMPathPrefix sets the IAMPathPrefix for the S3Repository
// This is used as the Path prefix for IAM resources
func WithIAMPathPrefix(prefix string) S3RepositoryOption {
	return func(s *S3Repository) {
		s.IAMPathPrefix = prefix
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

// bucketEmpty lists the objects in a bucket with a max of 1, if there are any objects returned, we return false
func (s *S3Repository) bucketEmpty(ctx context.Context, bucketName string) (bool, error) {
	if bucketName == "" {
		return false, apierror.New(apierror.ErrBadRequest, "invalid input", nil)
	}

	log.Debugf("checking if bucket %s is empty", bucketName)

	out, err := s.S3.ListObjectsV2WithContext(ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucketName),
		MaxKeys: aws.Int64(1),
	})
	if err != nil {
		return false, ErrCode("failed to determine if bucket is empty for bucket "+bucketName, err)
	}

	if aws.Int64Value(out.KeyCount) > 0 {
		return false, nil
	}

	return true, nil
}

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

// Describe returns information about the data repository
func (s *S3Repository) Describe(ctx context.Context, id string) (*dataset.Repository, error) {
	if id == "" {
		return nil, apierror.New(apierror.ErrBadRequest, "invalid input", errors.New("empty id"))
	}

	name := id
	if s.NamePrefix != "" {
		name = s.NamePrefix + "-" + name
	}

	log.Debugf("describing s3datarepository: %s", name)

	// check if bucket exists
	exists, err := s.bucketExists(ctx, name)
	if !exists {
		return nil, apierror.New(apierror.ErrNotFound, "s3 bucket not found: "+name, nil)
	} else if err != nil {
		return nil, apierror.New(apierror.ErrInternalError, "internal error", nil)
	}

	// check if there are any objects in the bucket
	empty, err := s.bucketEmpty(ctx, name)
	if err != nil {
		return nil, err
	}

	// get tags
	log.Debugf("getting tags for bucket %s", name)
	datasetTags, err := s.S3.GetBucketTaggingWithContext(ctx, &s3.GetBucketTaggingInput{Bucket: aws.String(name)})
	if err != nil {
		return nil, ErrCode("failed to get tags for s3 bucket "+name, err)
	}

	// prepare tags
	tags := make([]*dataset.Tag, len(datasetTags.TagSet))
	for i, tag := range datasetTags.TagSet {
		tags[i] = &dataset.Tag{
			Key:   tag.Key,
			Value: tag.Value,
		}
	}

	output := &dataset.Repository{
		Name:  name,
		Empty: empty,
		Tags:  tags,
	}

	return output, nil
}

// Provision creates and configures a data repository in S3, and creates a default IAM policy
// 1. Check if the requested bucket already exists in S3
// 2. Create the bucket and wait for it to be successfully created
// 3. Block all public access to the bucket
// 4. Enable AWS managed serverside encryption (AES-256) for the bucket
// 5. Add tags to the bucket
func (s *S3Repository) Provision(ctx context.Context, id string, datasetTags []*dataset.Tag) (string, error) {
	if id == "" {
		return "", apierror.New(apierror.ErrBadRequest, "invalid input", errors.New("empty id"))
	}

	name := id
	if s.NamePrefix != "" {
		name = s.NamePrefix + "-" + name
	}

	log.Debugf("provisioning s3datarepository: %s", name)

	// checks if a bucket exists in the account
	// in us-east-1 (only) bucket creation will succeed if the bucket already exists in your
	// account, but in all other regions the API will return s3.ErrCodeBucketAlreadyOwnedByYou 🤷‍♂️
	if exists, err := s.bucketExists(ctx, name); exists {
		return "", apierror.New(apierror.ErrConflict, "s3 bucket already exists", nil)
	} else if err != nil {
		return "", apierror.New(apierror.ErrInternalError, "internal error", nil)
	}

	// prepare tags
	tags := make([]*s3.Tag, len(datasetTags))
	for i, tag := range datasetTags {
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
		return "", ErrCode("failed to create s3 bucket "+name, err)
	}

	// append bucket delete to rollback tasks
	rollBackTasks = append(rollBackTasks, func() error {
		return func() error {
			log.Debugf("deleting s3 bucket: %s", name)
			if _, err := s.S3.DeleteBucketWithContext(ctx, &s3.DeleteBucketInput{Bucket: aws.String(name)}); err != nil {
				return err
			}
			return nil
		}()
	})

	// wait for bucket to exist
	if err = s.S3.WaitUntilBucketExistsWithContext(ctx, &s3.HeadBucketInput{Bucket: aws.String(name)},
		request.WithWaiterDelay(request.ConstantWaiterDelay(2*time.Second)),
	); err != nil {
		msg := fmt.Sprintf("failed to create bucket %s, timeout waiting for create: %s", name, err.Error())
		return "", apierror.New(apierror.ErrInternalError, msg, err)
	}

	log.Debugf("s3 bucket %s created successfully", name)

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
		return "", ErrCode("failed to block public access for s3 bucket "+name, err)
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
		return "", ErrCode("failed to enable encryption for s3 bucket "+name, err)
	}

	// add tags
	if len(tags) > 0 {
		log.Debugf("adding tags for bucket '%s': %+v", name, tags)
		if _, err = s.S3.PutBucketTaggingWithContext(ctx, &s3.PutBucketTaggingInput{
			Bucket:  aws.String(name),
			Tagging: &s3.Tagging{TagSet: tags},
		}); err != nil {
			return "", ErrCode("failed to tag s3 bucket "+name, err)
		}
	}

	return name, nil
}

// SetPolicy sets (or updates) the IAM access policy for the data repository, depending if it's a derivative or not
func (s *S3Repository) SetPolicy(ctx context.Context, id string, derivative bool) error {
	if id == "" {
		return apierror.New(apierror.ErrBadRequest, "invalid input", errors.New("empty id"))
	}

	name := id
	if s.NamePrefix != "" {
		name = s.NamePrefix + "-" + name
	}

	exists, err := s.policyExists(ctx, id)
	if err != nil {
		return ErrCode("failed to check if policy exists for s3 bucket "+name, err)
	}

	if exists {
		log.Infof("modifying existing access policy for bucket %s (derivative: %t)", name, derivative)
		if err = s.modifyPolicy(ctx, id, derivative); err != nil {
			return ErrCode("failed to modify access policy for s3 bucket "+name, err)
		}
	} else {
		log.Infof("creating new access policy for bucket %s (derivative: %t)", name, derivative)
		if err = s.createPolicy(ctx, id, derivative); err != nil {
			return ErrCode("failed to create access policy for s3 bucket "+name, err)
		}
	}

	return nil
}

// Deprovision satisfies the ability to deprovision a data repository
func (s *S3Repository) Deprovision(ctx context.Context, id string) error {
	if id == "" {
		return apierror.New(apierror.ErrBadRequest, "invalid input", errors.New("empty id"))
	}

	name := id
	if s.NamePrefix != "" {
		name = s.NamePrefix + "-" + name
	}

	log.Debugf("deprovisioning s3datarepository: %s", name)

	return nil
}

// Delete deletes a data repository in S3 and its associated IAM policy
func (s *S3Repository) Delete(ctx context.Context, id string) error {
	if id == "" {
		return apierror.New(apierror.ErrBadRequest, "invalid input", errors.New("empty id"))
	}

	name := id
	if s.NamePrefix != "" {
		name = s.NamePrefix + "-" + name
	}

	log.Infof("deleting s3datarepository: %s", name)

	// delete the s3 bucket
	_, err := s.S3.DeleteBucketWithContext(ctx, &s3.DeleteBucketInput{Bucket: aws.String(name)})
	if err != nil {
		return ErrCode("failed to delete s3 bucket "+name, err)
	}

	// delete associated dataset access policy
	log.Debugf("deleting dataset access policy for %s", id)
	if err = s.deletePolicy(ctx, id); err != nil {
		log.Warnf("failed to delete access policy for s3 bucket %s: %s", id, err)
	}

	return nil
}

type stop struct {
	error
}

// rollBack executes functions from a stack of rollback functions
func rollBack(t *[]func() error) {
	if t == nil {
		return
	}

	tasks := *t
	log.Errorf("executing rollback of %d tasks", len(tasks))
	for i := len(tasks) - 1; i >= 0; i-- {
		f := tasks[i]
		if funcerr := f(); funcerr != nil {
			log.Errorf("rollback task error: %s, continuing rollback", funcerr)
		}
	}
}

// retry is stolen from https://upgear.io/blog/simple-golang-retry-function/
func retry(attempts int, sleep time.Duration, f func() error) error {
	if err := f(); err != nil {
		if s, ok := err.(stop); ok {
			// Return the original error for later checking
			return s.error
		}

		if attempts--; attempts > 0 {
			// Add some randomness to prevent creating a Thundering Herd
			jitter := time.Duration(rand.Int63n(int64(sleep)))
			sleep = sleep + jitter/2

			time.Sleep(sleep)
			return retry(attempts, 2*sleep, f)
		}
		return err
	}

	return nil
}
