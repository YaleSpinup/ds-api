package dataset

import (
	"context"
	"mime/multipart"
	"time"

	"github.com/google/uuid"
)

// Service is a collection of the following:
// - a Metadata Repository for storing dataset metadata
// - an Audit Log Repository for storing audit logs
// - one or more Data Repositories for storing datasets
// - one or more Attachment Repositories for storing attachments
type Service struct {
	MetadataRepository   MetadataRepository
	AuditLogRepository   AuditLogRepository
	DataRepository       map[string]DataRepository
	AttachmentRepository map[string]AttachmentRepository
}

// MetadataRepository is an interface for metadata repository
type MetadataRepository interface {
	Create(ctx context.Context, account, id string, metadata *Metadata) (*Metadata, error)
	Get(ctx context.Context, account, id string) (*Metadata, error)
	Promote(ctx context.Context, account, id, user string) (*Metadata, error)
	Update(ctx context.Context, account, id string, metadata *Metadata) (*Metadata, error)
	Delete(ctx context.Context, account, id string) error
}

// AuditLogRepository is an interface for audit log repository
type AuditLogRepository interface {
	CreateLog(ctx context.Context, group, stream string, retention int64, tags []*Tag) error
	Log(ctx context.Context, account, id string) chan string
}

// DataRepository is an interface for data repository
type DataRepository interface {
	Provision(ctx context.Context, id string, tags []*Tag) (string, error)
	Deprovision(ctx context.Context, id string) error
	Delete(ctx context.Context, id string) error
	Describe(ctx context.Context, id string) (*Repository, error)
	SetPolicy(ctx context.Context, id string, derivative bool) error
	GrantAccess(ctx context.Context, id, instanceID string) (Access, error)
	ListAccess(ctx context.Context, id string) (Access, error)
	RevokeAccess(ctx context.Context, id, instanceID string) error
	CreateUser(ctx context.Context, id string) (interface{}, error)
	DeleteUser(ctx context.Context, id string) error
	ListUsers(ctx context.Context, id string) (map[string]interface{}, error)
	UpdateUser(ctx context.Context, id string) (map[string]interface{}, error)
}

// AttachmentRepository is an interface for attachment repository
type AttachmentRepository interface {
	CreateAttachment(ctx context.Context, id, attachmentName string, attachmentBody multipart.File) error
	DeleteAttachment(ctx context.Context, id, attachmentName string) error
	ListAttachments(ctx context.Context, id string, showURL bool) ([]Attachment, error)
}

// Access contains necessary information in order to access a dataset
type Access map[string]string

// Attachment contains information about a dataset attachment
type Attachment struct {
	Name     string
	Modified time.Time
	Size     int64
	URL      string `json:",omitempty"`
}

// ServiceOption is a function to set service options
type ServiceOption func(*Service)

// NewService creates a new dataset service with the provided ServiceOption functions
func NewService(opts ...ServiceOption) *Service {
	s := Service{}

	for _, opt := range opts {
		opt(&s)
	}

	return &s
}

// WithAuditLogRepository sets the AuditLogRepository for the service
func WithAuditLogRepository(repo AuditLogRepository) ServiceOption {
	return func(s *Service) {
		s.AuditLogRepository = repo
	}
}

// WithMetadataRepository sets the MetadataRepository for the service
func WithMetadataRepository(repo MetadataRepository) ServiceOption {
	return func(s *Service) {
		s.MetadataRepository = repo
	}
}

// WithDataRepository sets the DataRepository list for the service
func WithDataRepository(repos map[string]DataRepository) ServiceOption {
	return func(s *Service) {
		s.DataRepository = repos
	}
}

// WithAttachmentRepository sets the AttachmentRepository list for the service
func WithAttachmentRepository(repos map[string]AttachmentRepository) ServiceOption {
	return func(s *Service) {
		s.AttachmentRepository = repos
	}
}

// NewID generates a new dataset id
func (s *Service) NewID() string {
	return uuid.New().String()
}
