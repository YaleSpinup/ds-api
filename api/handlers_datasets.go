package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/YaleSpinup/ds-api/apierror"
	"github.com/YaleSpinup/ds-api/dataset"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
)

// DatasetCreateHandler creates a new "dataset"
// * generates an internal dataset id
// * creates the dataset repository
// * creates the metadata in the metadata repository
func (s *server) DatasetCreateHandler(w http.ResponseWriter, r *http.Request) {
	w = LogWriter{w}
	vars := mux.Vars(r)
	account := vars["account"]

	service, ok := s.datasetServices[account]
	if !ok {
		log.Errorf("account not found: %s", account)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	input := struct {
		Name       string            `json:"name"`
		Type       string            `json:"type"`
		Derivative bool              `json:"derivative"`
		Tags       []*dataset.Tag    `json:"tags"`
		Metadata   *dataset.Metadata `json:"metadata"`
	}{}

	err := json.NewDecoder(r.Body).Decode(&input)
	if err != nil {
		msg := fmt.Sprintf("cannot decode body into create dataset input: %s", err)
		handleError(w, apierror.New(apierror.ErrBadRequest, msg, err))
		return
	}

	log.Infof("creating data set (derivative: %t) in account '%s'", input.Derivative, account)

	if input.Name == "" {
		handleError(w, apierror.New(apierror.ErrBadRequest, "dataset name is required", nil))
		return
	}

	if input.Type == "" {
		handleError(w, apierror.New(apierror.ErrBadRequest, "dataset type is required", nil))
		return
	}

	dataRepo, ok := service.DataRepository[input.Type]
	if !ok {
		msg := fmt.Sprintf("requested dataset type not supported for this account: %s", input.Type)
		handleError(w, apierror.New(apierror.ErrBadRequest, msg, nil))
		return
	}

	log.Debugf("decoded request body into data set input %+v", input)

	id := service.NewID()

	log.Debugf("generated random id %s for new data set", id)

	// override metadata ID, Name, DataStorage and Derivative
	input.Metadata.ID = id
	input.Metadata.Name = input.Name
	input.Metadata.DataStorage = input.Type
	input.Metadata.Derivative = input.Derivative

	// set tags for ID, Name, Org
	// TODO: tag value validation, including the Name
	// In general, allowed characters in tags are letters, numbers, spaces representable in UTF-8, and the following characters: . : + = @ _ / - (hyphen).
	newTags := []*dataset.Tag{
		&dataset.Tag{
			Key:   aws.String("ID"),
			Value: aws.String(id),
		},
		&dataset.Tag{
			Key:   aws.String("Name"),
			Value: aws.String(input.Name),
		},
		&dataset.Tag{
			Key:   aws.String("spinup:org"),
			Value: aws.String(Org),
		},
	}
	for _, t := range input.Tags {
		if aws.StringValue(t.Key) != "ID" && aws.StringValue(t.Key) != "Name" && aws.StringValue(t.Key) != "spinup:org" {
			newTags = append(newTags, t)
		}
	}
	input.Tags = newTags

	// setup rollback function list and defer execution, note that we depend on the err variable defined above this
	var rollBackTasks []func() error
	defer func() {
		if err != nil {
			log.Errorf("recovering from error creating dataset: %s, executing %d rollback tasks", err, len(rollBackTasks))
			rollBack(&rollBackTasks)
		}
	}()

	// create dataset storage location
	var dataRepoName string
	log.Infof("provisioning dataset repository for %s", id)
	dataRepoName, err = dataRepo.Provision(r.Context(), id, input.Tags)
	if err != nil {
		handleError(w, err)
		return
	}

	// append dataset cleanup to rollback tasks
	rollBackTasks = append(rollBackTasks, func() error {
		return func() error {
			if err := dataRepo.Delete(r.Context(), id); err != nil {
				return err
			}
			return nil
		}()
	})

	// grant appropriate dataset access
	var datasetAccess dataset.Access
	log.Infof("granting dataset access for %s (derivative: %t)", id, input.Derivative)
	datasetAccess, err = dataRepo.GrantAccess(r.Context(), id, input.Derivative)
	if err != nil {
		handleError(w, err)
		return
	}

	// append access cleanup to rollback tasks
	rollBackTasks = append(rollBackTasks, func() error {
		return func() error {
			if err := dataRepo.RevokeAccess(r.Context(), id); err != nil {
				return err
			}
			return nil
		}()
	})

	// create metadata in repository
	log.Infof("adding dataset metadata for %s", id)
	metadataOutput, err := service.MetadataRepository.Create(r.Context(), account, id, input.Metadata)
	if err != nil {
		handleError(w, err)
		return
	}

	output := struct {
		ID         string            `json:"id"`
		Repository string            `json:"repository"`
		Metadata   *dataset.Metadata `json:"metadata"`
		Access     dataset.Access    `json:"access"`
	}{
		id,
		dataRepoName,
		metadataOutput,
		datasetAccess,
	}

	j, err := json.Marshal(&output)
	if err != nil {
		msg := fmt.Sprintf("cannot encode dataset output into json: %s", err)
		handleError(w, apierror.New(apierror.ErrBadRequest, msg, err))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(j)
}

func (s *server) DatasetListHandler(w http.ResponseWriter, r *http.Request) {
	w = LogWriter{w}
	vars := mux.Vars(r)
	account := vars["account"]

	log.Debugf("listing data sets for account %s", account)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotImplemented)
	w.Write([]byte{})
}

// DatasetShowHandler returns information about a a dataset
func (s *server) DatasetShowHandler(w http.ResponseWriter, r *http.Request) {
	w = LogWriter{w}
	vars := mux.Vars(r)
	account := vars["account"]
	id := vars["id"]

	service, ok := s.datasetServices[account]
	if !ok {
		log.Errorf("account not found: %s", account)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	log.Debugf("showing data set %s for account %s", id, account)

	// get metadata from repository
	metadataOutput, err := service.MetadataRepository.Get(r.Context(), account, id)
	if err != nil {
		handleError(w, err)
		return
	}

	// TODO: How to handle disabled/archived datasets?

	dataRepo, ok := service.DataRepository[metadataOutput.DataStorage]
	if !ok {
		msg := fmt.Sprintf("requested data repository type not supported for this account: %s", metadataOutput.DataStorage)
		handleError(w, apierror.New(apierror.ErrBadRequest, msg, nil))
		return
	}

	dataRepoOutput, err := dataRepo.Describe(r.Context(), id)
	if err != nil {
		msg := fmt.Sprintf("failed to describe data repository for dataset %s", id)
		handleError(w, apierror.New(apierror.ErrInternalError, msg, err))
		return
	}

	// TODO: We probably want to return access information here, although we need to clarify what exactly -
	// maybe just the role that has access to the data repository
	// Also, the Access struct should probably be part of the Repository
	output := struct {
		ID         string              `json:"id"`
		Metadata   *dataset.Metadata   `json:"metadata"`
		Repository *dataset.Repository `json:"repository"`
		// Access     *dataset.Access   `json:"access"`
	}{
		id,
		metadataOutput,
		dataRepoOutput,
		// datasetAccess,
	}

	j, err := json.Marshal(&output)
	if err != nil {
		msg := fmt.Sprintf("cannot encode dataset output into json: %s", err)
		handleError(w, apierror.New(apierror.ErrBadRequest, msg, err))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(j)
}

func (s *server) DatasetUpdateHandler(w http.ResponseWriter, r *http.Request) {
	w = LogWriter{w}
	vars := mux.Vars(r)
	account := vars["account"]
	dataset := vars["id"]

	log.Debugf("updating data set %s for account %s", dataset, account)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotImplemented)
	w.Write([]byte{})
}

func (s *server) DatasetDeleteHandler(w http.ResponseWriter, r *http.Request) {
	w = LogWriter{w}
	vars := mux.Vars(r)
	account := vars["account"]
	id := vars["id"]

	service, ok := s.datasetServices[account]
	if !ok {
		log.Errorf("account not found: %s", account)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	log.Debugf("deleting data set %s for account %s", id, account)

	// get metadata from repository
	metadataOutput, err := service.MetadataRepository.Get(r.Context(), account, id)
	if err != nil {
		handleError(w, err)
		return
	}

	dataRepo, ok := service.DataRepository[metadataOutput.DataStorage]
	if !ok {
		msg := fmt.Sprintf("requested data repository type not supported for this account: %s", metadataOutput.DataStorage)
		handleError(w, apierror.New(apierror.ErrBadRequest, msg, nil))
		return
	}

	// revoke access to the data repository (cleans up associated IAM policies)
	if err := dataRepo.RevokeAccess(r.Context(), id); err != nil {
		msg := fmt.Sprintf("failed to revoke data repository access for dataset %s", id)
		handleError(w, apierror.New(apierror.ErrInternalError, msg, err))
		return
	}

	// delete metadata
	// TODO: Maybe we just mark the dataset as deleted in the metadata
	if err = service.MetadataRepository.Delete(r.Context(), account, id); err != nil {
		msg := fmt.Sprintf("failed to delete metadata for dataset %s", id)
		handleError(w, apierror.New(apierror.ErrInternalError, msg, err))
		return
	}

	// delete data repository (needs to be empty)
	// Currently, this will only delete an empty bucket, or move on otherwise
	if err = dataRepo.Delete(r.Context(), id); err != nil {
		msg := fmt.Sprintf("failed to delete data repository for dataset %s", id)
		log.Warn(msg)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNoContent)
	w.Write([]byte{})
}
