package cwauditlogrepository

import (
	"context"
	"time"

	"github.com/YaleSpinup/ds-api/cloudwatchlogs"
	"github.com/YaleSpinup/ds-api/dataset"
	"github.com/aws/aws-sdk-go/aws"
	log "github.com/sirupsen/logrus"
)

type cwlogsIface interface {
	LogEvent(ctx context.Context, group, stream string, events []*cloudwatchlogs.Event) error
	CreateLogGroup(ctx context.Context, group string, tags map[string]*string) error
	UpdateRetention(ctx context.Context, group string, retention int64) error
	CreateLogStream(ctx context.Context, group, stream string) error
	TagLogGroup(ctx context.Context, group string, tags map[string]*string) error
	GetLogGroupTags(ctx context.Context, group string) (map[string]*string, error)
	DescribeLogGroup(ctx context.Context, group string) (*cloudwatchlogs.LogGroup, error)
	DeleteLogGroup(ctx context.Context, group string) error
}

// CWRepositoryOption is a function to set cloudwatch repository options
type CWRepositoryOption func(*CWAuditLogRepository)

// CWAuditLogRepository is an implementation of an audit respository in CloudWatch
type CWAuditLogRepository struct {
	CW      cwlogsIface
	Prefix  string
	timeout time.Duration
}

// NewDefaultRepository creates a new repository from the default config data
func NewDefaultRepository(config map[string]interface{}) (*CWAuditLogRepository, error) {
	var akid, secret, region string
	if v, ok := config["akid"].(string); ok {
		akid = v
	}

	if v, ok := config["secret"].(string); ok {
		secret = v
	}

	if v, ok := config["region"].(string); ok {
		region = v
	}

	opts := []CWRepositoryOption{
		WithClient(akid, secret, region),
	}

	// set default prefix
	opts = append(opts, WithPrefix("dataset"))

	// set default timeout
	opts = append(opts, WithTimeout(5*time.Minute))

	return New(opts...)
}

// New creates an CWAuditLogRepository from a list of CWRepositoryOption functions
func New(opts ...CWRepositoryOption) (*CWAuditLogRepository, error) {
	log.Info("creating new cloudwatch audit log repository provider")

	l := CWAuditLogRepository{}

	for _, opt := range opts {
		opt(&l)
	}

	return &l, nil
}

// WithClient initializes a cloudwatch client using AWS static credentials (key, secret, region)
func WithClient(akid, secret, region string) CWRepositoryOption {
	return func(l *CWAuditLogRepository) {
		log.Debugf("initializing cloudwatch client with akid %s", akid)
		cwClient := cloudwatchlogs.NewSession(region, akid, secret)
		l.CW = &cwClient
	}
}

// WithPrefix sets the log group prefix
func WithPrefix(prefix string) CWRepositoryOption {
	return func(l *CWAuditLogRepository) {
		log.Debugf("setting log group prefix %s", prefix)
		l.Prefix = prefix
	}
}

// WithTimeout sets the timeout
func WithTimeout(timeout time.Duration) CWRepositoryOption {
	return func(l *CWAuditLogRepository) {
		log.Debugf("setting audit log timeout %v", timeout)
		l.timeout = timeout
	}
}

// Log creates a channel for writing audit log events to the specified group and stream in CloudWatch
func (l *CWAuditLogRepository) Log(ctx context.Context, group, stream string) chan string {
	messageStream := make(chan string)

	// prepend the prefix to the given log group
	if l.Prefix != "" {
		group = l.Prefix + "-" + group
	}

	// TODO: this will fail if there are more than 10,000 entries batched.  Initially, I
	// handled this case, but I don't think we'll ever need it (and we can add the complexity
	// then if we do).  Removing the logic, makes this much simpler.
	go func() {
		log.Debugf("starting log batching go routine")

		// default to 10 minutes
		timeout := 10 * time.Minute
		if l.timeout != 0 {
			timeout = l.timeout
		}

		messages := []*cloudwatchlogs.Event{}

		defer func() {
			log.Debug("finalizing log batch")

			logctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			if len(messages) > 0 {
				for _, m := range messages {
					log.Debugf("sending log event to %s/%s: %d %s", group, stream, m.Timestamp, m.Message)
				}

				if err := l.CW.LogEvent(logctx, group, stream, messages); err != nil {
					log.Errorf("failed to log events: %s", err)
				}
			}
		}()

		for {
			log.Debug("starting log batch collection loop")
			select {
			case message := <-messageStream:
				timestamp := time.Now().UnixNano() / int64(time.Millisecond)
				log.Debugf("%d received message %s", timestamp, message)
				messages = append(messages, &cloudwatchlogs.Event{
					Message:   message,
					Timestamp: timestamp,
				})
			case <-time.After(timeout):
				log.Warnf("timed out waiting for more log messages to write to %s/%s", group, stream)
				return
			case <-ctx.Done():
				log.Debug("context closed")
				return
			}
		}
	}()

	return messageStream
}

// CreateLog creates the specified log group and stream in CloudWatch if they don't exist
// It also sets the log retention for the log group (in days) and adds tags
func (l *CWAuditLogRepository) CreateLog(ctx context.Context, group, stream string, retention int64, tags []*dataset.Tag) error {
	logGroup := group
	if l.Prefix != "" {
		logGroup = l.Prefix + "-" + logGroup
	}

	log.Infof("creating cloudwatch log %s/%s (%d day retention)", logGroup, stream, retention)

	// prepare tags
	tagsMap := make(map[string]*string, len(tags))
	for _, tag := range tags {
		tagsMap[aws.StringValue(tag.Key)] = tag.Value
	}

	// setup rollback function list and defer execution
	var err error
	var rollBackTasks []func() error
	defer func() {
		if err != nil {
			log.Errorf("recovering from error creating log: %s, executing %d rollback tasks", err, len(rollBackTasks))
			rollBack(&rollBackTasks)
		}
	}()

	if err = l.CW.CreateLogGroup(ctx, logGroup, tagsMap); err != nil {
		return err
	}

	// append job cleanup to rollback tasks
	rollBackTasks = append(rollBackTasks, func() error {
		return func() error {
			if err := l.CW.DeleteLogGroup(ctx, logGroup); err != nil {
				return err
			}
			return nil
		}()
	})

	if err = l.CW.UpdateRetention(ctx, logGroup, retention); err != nil {
		return err
	}

	if err = l.CW.CreateLogStream(ctx, logGroup, stream); err != nil {
		return err
	}

	return nil
}

func (l *CWAuditLogRepository) updateLog(ctx context.Context, group string, retention int64, tags []*dataset.Tag) error {
	logGroup := group
	if l.Prefix != "" {
		logGroup = l.Prefix + "-" + logGroup
	}

	// prepare tags
	tagsMap := make(map[string]*string, len(tags))
	for _, tag := range tags {
		tagsMap[aws.StringValue(tag.Key)] = tag.Value
	}

	if err := l.CW.UpdateRetention(ctx, logGroup, retention); err != nil {
		return err
	}

	if err := l.CW.TagLogGroup(ctx, logGroup, tagsMap); err != nil {
		return err
	}

	return nil
}

func (l *CWAuditLogRepository) describeLog(ctx context.Context, group string) (*cloudwatchlogs.LogGroup, []*dataset.Tag, error) {
	logGroup := group
	if l.Prefix != "" {
		logGroup = l.Prefix + "-" + logGroup
	}

	tags, err := l.CW.GetLogGroupTags(ctx, logGroup)
	if err != nil {
		return nil, nil, err
	}

	// prepare tags
	tagsList := make([]*dataset.Tag, 0, len(tags))
	for k, v := range tags {
		tagsList = append(tagsList, &dataset.Tag{
			Key:   aws.String(k),
			Value: v,
		})
	}

	lg, err := l.CW.DescribeLogGroup(ctx, logGroup)
	if err != nil {
		return nil, nil, err
	}

	return lg, tagsList, nil
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
