package dataset

import (
	"github.com/google/uuid"
)

// Service is a collection of one or more Data Repositories and a Matadata Repository for storing datasets
type Service struct {
	MetadataRepository MetadataRepository
	DataRepository     map[string]DataRepository
}

// MetadataRepository is an interface for metadata repository
type MetadataRepository interface {
	Create(id string, data []byte) error
	Get(id string) ([]byte, error)
	Update(id string, data []byte) error
	Delete(id string) error
}

// DataRepository is an interface for data repository
type DataRepository interface {
	Provision(id string) error
	Deprovision(id string) error
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
