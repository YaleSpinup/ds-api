package api

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/YaleSpinup/ds-api/common"
	"github.com/YaleSpinup/ds-api/cwauditlogrepository"
	"github.com/YaleSpinup/ds-api/dataset"
	"github.com/YaleSpinup/ds-api/s3datarepository"
	"github.com/YaleSpinup/ds-api/s3metadatarepository"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"

	log "github.com/sirupsen/logrus"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type server struct {
	datasetServices map[string]*dataset.Service
	router          *mux.Router
	version         common.Version
	context         context.Context
}

// Org will carry throughout the api and get tagged on resources
var Org string

// NewServer creates a new server and starts it
func NewServer(config common.Config) error {
	// setup server context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := server{
		datasetServices: make(map[string]*dataset.Service),
		router:          mux.NewRouter(),
		version:         config.Version,
		context:         ctx,
	}

	if config.Org == "" {
		return errors.New("'org' cannot be empty in the configuration")
	}
	Org = config.Org
	metadata := config.MetadataRepository

	// Initialize metadata repository session
	log.Debugf("Creating new session for MetadataRepository of type %s with configuration %+v (org: %s)", metadata.Type, metadata.Config, Org)

	var metadataRepo dataset.MetadataRepository
	var err error

	switch metadata.Type {
	case "s3":
		prefix := Org
		if c, ok := metadata.Config["prefix"]; ok {
			if p, ok := c.(string); ok {
				prefix = p + "/" + prefix
			}
		}
		metadata.Config["prefix"] = prefix

		metadataRepo, err = s3metadatarepository.NewDefaultRepository(metadata.Config)
		if err != nil {
			return err
		}
	default:
		return errors.New("failed to determine metadata repository type, or type not supported: " + metadata.Type)
	}

	// Create dataset service sessions
	for name, a := range config.Accounts {
		log.Debugf("Creating new service for account '%s' with key '%s' in region '%s' (org: %s, providers: %s)", name, a.Config["akid"], a.Config["region"], Org, a.StorageProviders)

		dataRepos := make(map[string]dataset.DataRepository)
		attachmentRepos := make(map[string]dataset.AttachmentRepository)

		if a.StorageProviders == nil || len(a.StorageProviders) == 0 {
			return errors.New("no storage providers configured for account: " + name)
		}

		// initialize all supported data storage providers for each account
		for _, p := range a.StorageProviders {
			switch p {
			case "s3":
				// configure the data repository
				dataRepo, err := s3datarepository.NewDefaultRepository(a.Config)
				if err != nil {
					return err
				}
				dataRepo.LoggingBucketPrefix = "dataset/" + Org + "/"
				dataRepo.NamePrefix = "dataset-" + Org
				dataRepos["s3"] = dataRepo

				// configure the attachment repository to use same backend as the data repository
				attachmentRepo, err := s3datarepository.NewDefaultRepository(a.Config)
				if err != nil {
					return err
				}
				attachmentRepo.NamePrefix = "dataset-" + Org
				attachmentRepos["s3"] = attachmentRepo
			default:
				msg := fmt.Sprintf("failed to determine data repository provider for account %s, or storage provider not supported: %s", name, p)
				return errors.New(msg)
			}
		}

		// Initialize audit log repository session and set log prefixes
		auditLogRepo, err := cwauditlogrepository.NewDefaultRepository(a.Config)
		if err != nil {
			return err
		}
		auditLogRepo.GroupPrefix = "/spinup/" + Org + "/"
		auditLogRepo.StreamPrefix = "dataset-"

		s.datasetServices[name] = dataset.NewService(
			dataset.WithAuditLogRepository(auditLogRepo),
			dataset.WithMetadataRepository(metadataRepo),
			dataset.WithDataRepository(dataRepos),
			dataset.WithAttachmentRepository(attachmentRepos),
		)
	}

	publicURLs := map[string]string{
		"/v1/ds/ping":    "public",
		"/v1/ds/version": "public",
		"/v1/ds/metrics": "public",
	}

	// load routes
	s.routes()

	if config.ListenAddress == "" {
		config.ListenAddress = ":8080"
	}
	handler := handlers.RecoveryHandler()(handlers.LoggingHandler(os.Stdout, TokenMiddleware([]byte(config.Token), publicURLs, s.router)))
	srv := &http.Server{
		Handler:      handler,
		Addr:         config.ListenAddress,
		WriteTimeout: 60 * time.Second,
		ReadTimeout:  60 * time.Second,
	}

	log.Infof("Starting listener on %s", config.ListenAddress)
	if err := srv.ListenAndServe(); err != nil {
		return err
	}

	return nil
}

// LogWriter is an http.ResponseWriter
type LogWriter struct {
	http.ResponseWriter
}

// Write log message if http response writer returns an error
func (w LogWriter) Write(p []byte) (n int, err error) {
	n, err = w.ResponseWriter.Write(p)
	if err != nil {
		log.Errorf("Write failed: %v", err)
	}
	return
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

type stop struct {
	error
}
