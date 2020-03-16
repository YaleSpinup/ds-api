package dataset

import (
	"context"

	"github.com/google/uuid"
)

// Service is a collection of one or more Data Repositories and a Matadata Repository for storing datasets
type Service struct {
	MetadataRepository MetadataRepository
	DataRepository     map[string]DataRepository
}

// MetadataRepository is an interface for metadata repository
type MetadataRepository interface {
	Create(ctx context.Context, account, id string, metadata *Metadata) (*Metadata, error)
	Get(ctx context.Context, account, id string) (*Metadata, error)
	Update(ctx context.Context, account, id string, metadata *Metadata) (*Metadata, error)
	Delete(ctx context.Context, account, id string) error
}

// DataRepository is an interface for data repository
type DataRepository interface {
	Provision(ctx context.Context, id string, tags []*Tag) (string, error)
	Deprovision(ctx context.Context, id string) error
	Delete(ctx context.Context, id string) error
	Describe(ctx context.Context, id string) (*Repository, error)
	GrantAccess(ctx context.Context, id string, derivative bool) (*Access, error)
	RevokeAccess(ctx context.Context, id string) error
}

// Access contains necessary information in order to access a dataset
type Access map[string]string

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

// NewID generates a new dataset id
func (s *Service) NewID() string {
	return uuid.New().String()
}
