package cwauditlogrepository

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/YaleSpinup/ds-api/cloudwatchlogs"
	"github.com/YaleSpinup/ds-api/dataset"
	"github.com/aws/aws-sdk-go/aws"
)

type mockCWLclient struct {
	t   *testing.T
	err error
}

type logGroup struct {
	name      string
	retention int64
	streams   map[string][]*cloudwatchlogs.Event
	tags      map[string]*string
}

var logGroupsMux sync.Mutex
var logGroups map[string]*logGroup

func (m *mockCWLclient) LogEvent(ctx context.Context, group, stream string, events []*cloudwatchlogs.Event) error {
	if m.err != nil {
		return m.err
	}

	for _, e := range events {
		m.t.Logf("logging event to %s/%s: %d %s", group, stream, e.Timestamp, e.Message)
	}

	// logging only unlocks mux when its done writing
	defer func() {
		m.t.Logf("unlocking log groups in LogEvent")
		logGroupsMux.Unlock()
	}()

	lg, ok := logGroups[group]
	if !ok {
		return errors.New("log group not found " + group)
	}

	logStream, ok := lg.streams[stream]
	if !ok {
		return errors.New("stream '" + stream + "' not found")
	}

	// append events to logs stream
	logStream = append(logStream, events...)
	lg.streams[stream] = logStream

	return nil
}

func (m *mockCWLclient) CreateLogGroup(ctx context.Context, group string, tags map[string]*string) error {
	if m.err != nil {
		return m.err
	}

	m.t.Logf("creating log group %s with tags %+v", group, tags)

	m.t.Log("locking log groups in CreateLogGroup")
	logGroupsMux.Lock()
	defer func() {
		m.t.Logf("unlocking log groups in CreateLogGroup")
		logGroupsMux.Unlock()
	}()

	if _, ok := logGroups[group]; ok {
		return errors.New("exists")
	}

	// create group
	logGroups[group] = &logGroup{
		name:    group,
		tags:    tags,
		streams: make(map[string][]*cloudwatchlogs.Event),
	}

	return nil
}

func (m *mockCWLclient) UpdateRetention(ctx context.Context, group string, retention int64) error {
	if m.err != nil {
		return m.err
	}

	m.t.Logf("updating log group %s retention to %d days", group, retention)

	m.t.Log("locking log groups in UpdateRetention")
	logGroupsMux.Lock()
	defer func() {
		m.t.Logf("unlocking log groups in UpdateRetention")
		logGroupsMux.Unlock()
	}()

	lg, ok := logGroups[group]
	if !ok {
		return errors.New("group not found " + group)
	}

	// set retention
	lg.retention = retention
	return nil
}

func (m *mockCWLclient) CreateLogStream(ctx context.Context, group, stream string) error {
	if m.err != nil {
		return m.err
	}

	m.t.Logf("creating log stream %s/%s", group, stream)

	m.t.Log("locking log groups in CreateLogStream")
	logGroupsMux.Lock()
	defer func() {
		m.t.Logf("unlocking log groups in CreateLogStream")
		logGroupsMux.Unlock()
	}()

	lg, ok := logGroups[group]
	if !ok {
		return errors.New("group not found: " + group)
	}

	if _, ok := lg.streams[stream]; ok {
		return errors.New("stream already exists in group")
	}

	// create stream
	lg.streams[stream] = []*cloudwatchlogs.Event{}
	return nil
}

func (m *mockCWLclient) GetLogEvents(ctx context.Context, group, stream string) ([]*cloudwatchlogs.Event, error) {
	return nil, nil
}

func (m *mockCWLclient) TagLogGroup(ctx context.Context, group string, tags map[string]*string) error {
	return nil
}

func (m *mockCWLclient) GetLogGroupTags(ctx context.Context, group string) (map[string]*string, error) {
	return nil, nil
}

func (m *mockCWLclient) DescribeLogGroup(ctx context.Context, group string) (*cloudwatchlogs.LogGroup, error) {
	return nil, nil
}

func (m *mockCWLclient) DeleteLogGroup(ctx context.Context, group string) error {
	return nil
}

func newMockCWAuditLogRepository(prefix string, timeout time.Duration, cwl *mockCWLclient) *CWAuditLogRepository {
	return &CWAuditLogRepository{
		CW:          cwl,
		GroupPrefix: prefix,
		timeout:     timeout,
	}
}

func TestNewDefaultRepository(t *testing.T) {
	testConfig := map[string]interface{}{
		"region": "us-east-1",
		"akid":   "xxxxx",
		"secret": "yyyyy",
	}

	s, err := NewDefaultRepository(testConfig)
	if err != nil {
		t.Errorf("expected nil error, got: %s", err)
	}
	to := reflect.TypeOf(s).String()
	if to != "*cwauditlogrepository.CWAuditLogRepository" {
		t.Errorf("expected type to be '*cwauditlogrepository.CWAuditLogRepository', got %s", to)
	}

	if s.GroupPrefix != "" {
		t.Errorf("expected GroupPrefix to be '', got %s", s.GroupPrefix)
	}

	if s.timeout != 5*time.Minute {
		t.Errorf("expected timeout to be 5 minutes, got %d", s.timeout)
	}
}

func TestNew(t *testing.T) {
	s, err := New()
	if err != nil {
		t.Errorf("expected nil error, got: %s", err)
	}
	to := reflect.TypeOf(s).String()
	if to != "*cwauditlogrepository.CWAuditLogRepository" {
		t.Errorf("expected type to be '*cwauditlogrepository.CWAuditLogRepository', got %s", to)
	}
}

func TestCreateLog(t *testing.T) {
	logGroups = make(map[string]*logGroup)
	l := newMockCWAuditLogRepository("/test/", 5*time.Second, &mockCWLclient{t: t})

	tags := []*dataset.Tag{
		{Key: aws.String("soClose"), Value: aws.String("noMatterHowFar")},
		{Key: aws.String("couldntBe"), Value: aws.String("muchMoreFromTheHeart")},
		{Key: aws.String("forever"), Value: aws.String("trustingWhoWeAre")},
		{Key: aws.String("andNothing"), Value: aws.String("elseMatters")},
	}

	expectedTags := make(map[string]*string)
	for _, tag := range tags {
		expectedTags[aws.StringValue(tag.Key)] = tag.Value
	}

	expected := &logGroup{
		name:      "/test/group",
		retention: int64(90),
		streams: map[string][]*cloudwatchlogs.Event{
			"test-stream": {},
		},
		tags: expectedTags,
	}

	if err := l.CreateLog(context.TODO(), "group", "test-stream", int64(90), tags); err != nil {
		t.Errorf("expected nil error, got %s", err)
	}

	if lg, ok := logGroups["/test/group"]; !ok {
		t.Error("expected log group '/test/group' to exist")
	} else {
		if !reflect.DeepEqual(lg, expected) {
			t.Errorf("expected %+v, got %+v", expected, lg)
		}
	}

	l = newMockCWAuditLogRepository("test", 5*time.Second, &mockCWLclient{t: t, err: errors.New("boom!")})
	if err := l.CreateLog(context.TODO(), "nonexistent-group", "test-stream", int64(90), tags); err == nil {
		t.Error("expected error for missing log-group, got nil")
	}
}

func TestLog(t *testing.T) {
	logGroups = make(map[string]*logGroup)
	testLogGroup := logGroup{
		name:      "test-group",
		retention: int64(365),
		streams: map[string][]*cloudwatchlogs.Event{
			"test-stream": {},
		},
	}
	logGroups = map[string]*logGroup{
		testLogGroup.name: &testLogGroup,
	}

	testMessages := []string{
		"some random message",
		"some random message",
		"some random message",
		"some random message",
		"some random message",
	}

	logGroupsMux.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	messageStream := newMockCWAuditLogRepository("", 5*time.Second, &mockCWLclient{t: t}).Log(ctx, "test-group", "test-stream")
	for _, m := range testMessages {
		messageStream <- m
	}
	cancel()

	// attempt to lock so we know the messages were written to the map
	logGroupsMux.Lock()
	for _, lg := range logGroups {
		t.Logf("log-group: %+v", lg)
	}

	s, ok := testLogGroup.streams["test-stream"]
	if !ok {
		t.Errorf("expected log stream 'test-stream' to exist")
	}

	resultMessages := []string{}
	for _, m := range s {
		resultMessages = append(resultMessages, m.Message)
	}

	if !reflect.DeepEqual(testMessages, resultMessages) {
		t.Errorf("expected: %+v, got %+v", testMessages, resultMessages)
	}
	logGroupsMux.Unlock()
}
