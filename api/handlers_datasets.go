package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/YaleSpinup/ds-api/apierror"
	"github.com/YaleSpinup/ds-api/dataset"
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

	log.Infof("creating data set for account %s", account)

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

	metadata, err := json.Marshal(&input.Metadata)
	if err != nil {
		msg := fmt.Sprintf("cannot encode metadata input: %s", err)
		handleError(w, apierror.New(apierror.ErrBadRequest, msg, err))
		return
	}

	// setup rollback function list and defer execution, note that we depend on the err variable defined above this
	var rollBackTasks []func() error
	defer func() {
		if err != nil {
			log.Errorf("recovering from error: %s, executing %d rollback tasks", err, len(rollBackTasks))
			rollBack(&rollBackTasks)
		}
	}()

	// create dataset storage location
	err = dataRepo.Provision(id)
	if err != nil {
		handleError(w, err)
		return
	}

	// create metadata in repository
	err = service.MetadataRepository.Create(id, metadata)
	if err != nil {
		handleError(w, err)
		return
	}

	// TODO: create IAM policy

	out, err := json.Marshal(&input)
	if err != nil {
		msg := fmt.Sprintf("cannot encode input back into dataset output: %s", err)
		handleError(w, apierror.New(apierror.ErrBadRequest, msg, err))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotImplemented)
	w.Write([]byte(out))
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
