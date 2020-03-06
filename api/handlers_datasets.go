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

	log.Infof("creating data set in account '%s'", account)

	input := struct {
		Name     string            `json:"name"`
		Type     string            `json:"type"`
		Tags     []*dataset.Tag    `json:"tags"`
		Metadata *dataset.Metadata `json:"metadata"`
	}{}

	err := json.NewDecoder(r.Body).Decode(&input)
	if err != nil {
		msg := fmt.Sprintf("cannot decode body into create dataset input: %s", err)
		handleError(w, apierror.New(apierror.ErrBadRequest, msg, err))
		return
	}

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

	// override metadata ID, Name and DataStorage
	input.Metadata.ID = id
	input.Metadata.Name = input.Name
	input.Metadata.DataStorage = input.Type

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
	log.Infof("provisioning dataset repository for %s", id)
	if err = dataRepo.Provision(r.Context(), id, input.Tags); err != nil {
		handleError(w, err)
		return
	}

	// append dataset cleanup to rollback tasks
	rbfunc := func() error {
		return func() error {
			if err := dataRepo.Delete(r.Context(), id); err != nil {
				return err
			}
			return nil
		}()
	}
	rollBackTasks = append(rollBackTasks, rbfunc)

	// create metadata in repository
	log.Infof("adding dataset metadata for %s", id)
	out, err := service.MetadataRepository.Create(r.Context(), account, id, input.Metadata)
	if err != nil {
		handleError(w, err)
		return
	}

	j, err := json.Marshal(&out)
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

func (s *server) DatasetShowHandler(w http.ResponseWriter, r *http.Request) {
	w = LogWriter{w}
	vars := mux.Vars(r)
	account := vars["account"]
	dataset := vars["id"]

	log.Debugf("showing data set %s for account %s", dataset, account)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotImplemented)
	w.Write([]byte{})
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
	dataset := vars["id"]

	log.Debugf("deleting data set %s for account %s", dataset, account)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotImplemented)
	w.Write([]byte{})
}
