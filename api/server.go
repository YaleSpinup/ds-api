package api

import (
	"context"
	"errors"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/YaleSpinup/ds-api/common"
	"github.com/YaleSpinup/ds-api/dataset"
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
	repo := config.Repository

	// Create metadata repository session
	log.Debugf("Creating new MetadataRepository of type %s with configuration %+v (org: %s)", repo.Type, repo.Config, Org)

	var metadataRepo dataset.MetadataRepository
	var err error
	switch repo.Type {
	case "s3":
		metadataRepo, err = s3metadatarepository.NewDefaultRepository(repo.Config)
		if err != nil {
			return err
		}
	default:
		return errors.New("failed to determine metadata repository type, or type not supported: " + repo.Type)
	}

	// Create a shared session
	for name, c := range config.Accounts {
		log.Debugf("Creating new service for account '%s' with key '%s' in region '%s' (org: %s)", name, c.Akid, c.Region, Org)
		s.datasetServices[name] = dataset.NewService(
			dataset.WithMetadataRepository(metadataRepo),
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
	handler := handlers.RecoveryHandler()(handlers.LoggingHandler(os.Stdout, TokenMiddleware(config.Token, publicURLs, s.router)))
	srv := &http.Server{
		Handler:      handler,
		Addr:         config.ListenAddress,
		WriteTimeout: 15 * time.Second,
		ReadTimeout:  15 * time.Second,
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
