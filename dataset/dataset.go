package dataset

import "github.com/google/uuid"

type ServiceOption func(*Service)
type Service struct {
	MetadataRepository MetadataRepository
	DataRepository     DataRepository
}
type MetadataRepository interface {
	Read(id string) ([]byte, error)
	Save(id string, data []byte) error
	Delete(id string) error
}

type DataRepository interface {
	Provision(id string) error
}

func NewService(opts ...ServiceOption) *Service {
	s := Service{}

	for _, opt := range opts {
		opt(&s)
	}

	return &Service{}
}

func WithMetadataRepository(repo MetadataRepository) ServiceOption {
	return func(s *Service) {
		s.MetadataRepository = repo
	}
}

func WithDataRepository(repo DataRepository) ServiceOption {
	return func(s *Service) {
		s.DataRepository = repo
	}
}

func (s *Service) NewID() string {
	return uuid.New().String()
}
