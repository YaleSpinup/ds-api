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
	group := vars["group"]

	service, ok := s.datasetServices[account]
	if !ok {
		msg := fmt.Sprintf("account not found: %s", account)
		handleError(w, apierror.New(apierror.ErrNotFound, msg, nil))
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

	// generate dataset access policy
	log.Infof("provisioning access policy for %s", id)
	if err = dataRepo.SetPolicy(r.Context(), id, input.Derivative); err != nil {
		handleError(w, err)
		return
	}

	// create metadata in repository
	log.Infof("adding dataset metadata for %s", id)
	metadataOutput, err := service.MetadataRepository.Create(r.Context(), account, id, input.Metadata)
	if err != nil {
		handleError(w, err)
		return
	}

	// append metadata cleanup to rollback tasks
	rollBackTasks = append(rollBackTasks, func() error {
		return func() error {
			if err := service.MetadataRepository.Delete(r.Context(), account, id); err != nil {
				return err
			}
			return nil
		}()
	})

	output := struct {
		ID         string            `json:"id"`
		Repository string            `json:"repository"`
		Metadata   *dataset.Metadata `json:"metadata"`
	}{
		id,
		dataRepoName,
		metadataOutput,
	}

	var j []byte
	j, err = json.Marshal(&output)
	if err != nil {
		msg := fmt.Sprintf("cannot encode dataset output into json: %s", err)
		handleError(w, apierror.New(apierror.ErrBadRequest, msg, err))
		return
	}

	// create new audit log for this data set, with a retention period of 365 days
	lErr := service.AuditLogRepository.CreateLog(r.Context(), group, id, int64(365), input.Tags)
	if lErr != nil {
		log.Errorf("failed creating job audit log for %s: %s", id, lErr)
	} else {
		// initialize audit log stream
		auditLog := service.AuditLogRepository.Log(r.Context(), group, id)
		msg := fmt.Sprintf("Created dataset %s (CreatedBy: %s)", id, metadataOutput.CreatedBy)
		auditLog <- msg
		auditLog <- string(j)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(j)
}

func (s *server) DatasetListHandler(w http.ResponseWriter, r *http.Request) {
	w = LogWriter{w}
	vars := mux.Vars(r)
	account := vars["account"]
	group := vars["group"]

	log.Debugf("listing data sets for account %s, group %s", account, group)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotImplemented)
	w.Write([]byte{})
}

// DatasetShowHandler returns information about a dataset
func (s *server) DatasetShowHandler(w http.ResponseWriter, r *http.Request) {
	w = LogWriter{w}
	vars := mux.Vars(r)
	account := vars["account"]
	id := vars["id"]

	service, ok := s.datasetServices[account]
	if !ok {
		msg := fmt.Sprintf("account not found: %s", account)
		handleError(w, apierror.New(apierror.ErrNotFound, msg, nil))
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

// DatasetPromoteHandler promotes a dataset
// If this is an original dataset, it will be finalized (if not already finalized)
// If this is a derivative, it will be promoted to an original and instantly finalized
func (s *server) DatasetPromoteHandler(w http.ResponseWriter, r *http.Request) {
	w = LogWriter{w}
	vars := mux.Vars(r)
	account := vars["account"]
	group := vars["group"]
	id := vars["id"]

	user := r.Header.Get("X-Forwarded-User")
	if user == "" {
		handleError(w, apierror.New(apierror.ErrBadRequest, "X-Forwarded-User header is required", nil))
		return
	}

	service, ok := s.datasetServices[account]
	if !ok {
		msg := fmt.Sprintf("account not found: %s", account)
		handleError(w, apierror.New(apierror.ErrNotFound, msg, nil))
		return
	}

	// get current metadata from repository
	metadata, err := service.MetadataRepository.Get(r.Context(), account, id)
	if err != nil {
		handleError(w, err)
		return
	}

	// if this is currently a derivative data set that is promoted to original
	// we update the access policy for the data repository
	if metadata.Derivative {
		dataRepo, ok := service.DataRepository[metadata.DataStorage]
		if !ok {
			msg := fmt.Sprintf("requested data repository type not supported for this account: %s", metadata.DataStorage)
			handleError(w, apierror.New(apierror.ErrBadRequest, msg, nil))
			return
		}

		if err = dataRepo.SetPolicy(r.Context(), id, false); err != nil {
			msg := fmt.Sprintf("failed to set access policy for dataset %s", id)
			handleError(w, apierror.New(apierror.ErrInternalError, msg, err))
			return
		}
	}

	// finalize repository metadata
	metadataOutput, err := service.MetadataRepository.Promote(r.Context(), account, id, user)
	if err != nil {
		handleError(w, err)
		return
	}

	output := struct {
		ID       string            `json:"id"`
		Metadata *dataset.Metadata `json:"metadata"`
	}{
		id,
		metadataOutput,
	}

	j, err := json.Marshal(&output)
	if err != nil {
		msg := fmt.Sprintf("cannot encode dataset output into json: %s", err)
		handleError(w, apierror.New(apierror.ErrBadRequest, msg, err))
		return
	}

	// write to audit log
	auditLog := service.AuditLogRepository.Log(r.Context(), group, id)
	if metadata.Derivative {
		auditLog <- fmt.Sprintf("Promoted derivative dataset %s to original (ModifiedBy: %s)", id, user)
	} else {
		auditLog <- fmt.Sprintf("Finalized original dataset %s (ModifiedBy: %s)", id, user)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(j)
}

// DatasetUpdateHandler updates metadata for a dataset
// Only the following fields can be updated: Description, ModifiedBy
func (s *server) DatasetUpdateHandler(w http.ResponseWriter, r *http.Request) {
	w = LogWriter{w}
	vars := mux.Vars(r)
	account := vars["account"]
	group := vars["group"]
	id := vars["id"]

	user := r.Header.Get("X-Forwarded-User")
	if user == "" {
		handleError(w, apierror.New(apierror.ErrBadRequest, "X-Forwarded-User header is required", nil))
		return
	}

	service, ok := s.datasetServices[account]
	if !ok {
		msg := fmt.Sprintf("account not found: %s", account)
		handleError(w, apierror.New(apierror.ErrNotFound, msg, nil))
		return
	}

	// TODO: currently we only support updating the metadata for a dataset, eventually we should update tags as well
	input := struct {
		Metadata *dataset.Metadata `json:"metadata"`
	}{}

	err := json.NewDecoder(r.Body).Decode(&input)
	if err != nil {
		msg := fmt.Sprintf("cannot decode body into update dataset input: %s", err)
		handleError(w, apierror.New(apierror.ErrBadRequest, msg, err))
		return
	}

	if input.Metadata == nil {
		handleError(w, apierror.New(apierror.ErrBadRequest, "dataset metadata is required", nil))
		return
	}

	log.Infof("updating data set %s for account %s by user %s", id, account, user)

	// get current metadata from repository
	metadata, err := service.MetadataRepository.Get(r.Context(), account, id)
	if err != nil {
		handleError(w, err)
		return
	}

	metadata.ModifiedBy = user

	// override allowed metadata fields
	if input.Metadata.Description != "" {
		metadata.Description = input.Metadata.Description
	}

	// update metadata
	metadataOutput, err := service.MetadataRepository.Update(r.Context(), account, id, metadata)
	if err != nil {
		msg := fmt.Sprintf("failed to delete metadata for dataset %s", id)
		handleError(w, apierror.New(apierror.ErrInternalError, msg, err))
		return
	}

	output := struct {
		ID       string            `json:"id"`
		Metadata *dataset.Metadata `json:"metadata"`
	}{
		id,
		metadataOutput,
	}

	j, err := json.Marshal(&output)
	if err != nil {
		msg := fmt.Sprintf("cannot encode dataset output into json: %s", err)
		handleError(w, apierror.New(apierror.ErrBadRequest, msg, err))
		return
	}

	// write to audit log
	auditLog := service.AuditLogRepository.Log(r.Context(), group, id)
	msg := fmt.Sprintf("Updated metadata for dataset %s (ModifiedBy: %s)", id, user)
	auditLog <- msg
	auditLog <- string(j)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(j)
}

func (s *server) DatasetDeleteHandler(w http.ResponseWriter, r *http.Request) {
	w = LogWriter{w}
	vars := mux.Vars(r)
	account := vars["account"]
	group := vars["group"]
	id := vars["id"]

	user := r.Header.Get("X-Forwarded-User")
	if user == "" {
		handleError(w, apierror.New(apierror.ErrBadRequest, "X-Forwarded-User header is required", nil))
		return
	}

	service, ok := s.datasetServices[account]
	if !ok {
		msg := fmt.Sprintf("account not found: %s", account)
		handleError(w, apierror.New(apierror.ErrNotFound, msg, nil))
		return
	}

	log.Infof("deleting data set %s for account %s by user %s", id, account, user)

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

	// delete data repository (needs to be empty)
	if err = dataRepo.Delete(r.Context(), id); err != nil {
		msg := fmt.Sprintf("failed to delete data repository for dataset %s", id)
		handleError(w, apierror.New(apierror.ErrInternalError, msg, err))
		return
	}

	// delete metadata
	if err = service.MetadataRepository.Delete(r.Context(), account, id); err != nil {
		msg := fmt.Sprintf("failed to delete metadata for dataset %s", id)
		handleError(w, apierror.New(apierror.ErrInternalError, msg, err))
		return
	}

	// write to audit log
	auditLog := service.AuditLogRepository.Log(r.Context(), group, id)
	msg := fmt.Sprintf("Deleted dataset %s (DeletedBy: %s)", id, user)
	auditLog <- msg

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNoContent)
	w.Write([]byte{})
}
