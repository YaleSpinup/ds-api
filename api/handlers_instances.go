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

// InstanceCreateHandler provisions access to a "dataset" for a given instance
func (s *server) InstanceCreateHandler(w http.ResponseWriter, r *http.Request) {
	w = LogWriter{w}
	vars := mux.Vars(r)
	account := vars["account"]
	group := vars["group"]
	id := vars["id"]

	service, ok := s.datasetServices[account]
	if !ok {
		msg := fmt.Sprintf("account not found: %s", account)
		handleError(w, apierror.New(apierror.ErrNotFound, msg, nil))
		return
	}

	input := struct {
		InstanceID string `json:"instance_id"`
	}{}

	err := json.NewDecoder(r.Body).Decode(&input)
	if err != nil {
		msg := fmt.Sprintf("cannot decode body into create instance input: %s", err)
		handleError(w, apierror.New(apierror.ErrBadRequest, msg, err))
		return
	}

	if input.InstanceID == "" {
		handleError(w, apierror.New(apierror.ErrBadRequest, "instance_id is required", nil))
		return
	}

	log.Infof("provisioning access to data set '%s' in account '%s' for instance: %s", id, account, input.InstanceID)

	metadata, err := service.MetadataRepository.Get(r.Context(), account, id)
	if err != nil {
		handleError(w, err)
		return
	}

	dataRepo, ok := service.DataRepository[metadata.DataStorage]
	if !ok {
		msg := fmt.Sprintf("requested data repository type not supported for this account: %s", metadata.DataStorage)
		handleError(w, apierror.New(apierror.ErrBadRequest, msg, nil))
		return
	}

	// grant access to this data repository
	datasetAccess, err := dataRepo.GrantAccess(r.Context(), id, input.InstanceID)
	if err != nil {
		msg := fmt.Sprintf("failed to grant access to data repository for dataset %s: %s", id, err)
		handleError(w, apierror.New(apierror.ErrInternalError, msg, err))
		return
	}

	output := struct {
		InstanceID string         `json:"instance_id"`
		Access     dataset.Access `json:"access"`
	}{
		input.InstanceID,
		datasetAccess,
	}

	j, err := json.Marshal(&output)
	if err != nil {
		msg := fmt.Sprintf("cannot encode dataset output into json: %s", err)
		handleError(w, apierror.New(apierror.ErrBadRequest, msg, err))
		return
	}

	// write to audit log
	auditLog := service.AuditLogRepository.Log(r.Context(), group, id)
	msg := fmt.Sprintf("Granted instance access to dataset %s (InstanceID: %s)", id, input.InstanceID)
	auditLog <- msg

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(j)
}

// InstanceListHandler lists all instances that have access to the dataset
func (s *server) InstanceListHandler(w http.ResponseWriter, r *http.Request) {
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

	log.Debugf("listing instances with access to data set '%s' in account %s", id, account)

	metadata, err := service.MetadataRepository.Get(r.Context(), account, id)
	if err != nil {
		handleError(w, err)
		return
	}

	dataRepo, ok := service.DataRepository[metadata.DataStorage]
	if !ok {
		msg := fmt.Sprintf("requested data repository type not supported for this account: %s", metadata.DataStorage)
		handleError(w, apierror.New(apierror.ErrBadRequest, msg, nil))
		return
	}

	// list access to this data repository
	datasetAccess, err := dataRepo.ListAccess(r.Context(), id)
	if err != nil {
		msg := fmt.Sprintf("failed to list access to data repository for dataset %s: %s", id, err)
		handleError(w, apierror.New(apierror.ErrInternalError, msg, err))
		return
	}

	output := struct {
		ID     string         `json:"id"`
		Access dataset.Access `json:"access"`
	}{
		id,
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

// InstanceDeleteHandler removes access to a "dataset" for a given instance
func (s *server) InstanceDeleteHandler(w http.ResponseWriter, r *http.Request) {
	w = LogWriter{w}
	vars := mux.Vars(r)
	account := vars["account"]
	group := vars["group"]
	id := vars["id"]
	instanceID := vars["instance_id"]

	service, ok := s.datasetServices[account]
	if !ok {
		msg := fmt.Sprintf("account not found: %s", account)
		handleError(w, apierror.New(apierror.ErrNotFound, msg, nil))
		return
	}

	log.Infof("revoking access to data set '%s' in account %s for instance: %s", id, account, instanceID)

	metadata, err := service.MetadataRepository.Get(r.Context(), account, id)
	if err != nil {
		handleError(w, err)
		return
	}

	dataRepo, ok := service.DataRepository[metadata.DataStorage]
	if !ok {
		msg := fmt.Sprintf("requested data repository type not supported for this account: %s", metadata.DataStorage)
		handleError(w, apierror.New(apierror.ErrBadRequest, msg, nil))
		return
	}

	// list current access to this data repository
	listAccess, err := dataRepo.ListAccess(r.Context(), id)
	if err != nil {
		msg := fmt.Sprintf("failed to list access to data repository for dataset %s: %s", id, err)
		handleError(w, apierror.New(apierror.ErrInternalError, msg, err))
		return
	}

	// check if requested instance currently has access
	if _, ok := listAccess[instanceID]; !ok {
		msg := fmt.Sprintf("instance %s currently does not have access to data repository for dataset %s", instanceID, id)
		handleError(w, apierror.New(apierror.ErrBadRequest, msg, nil))
		return
	}

	// revoke access to this data repository
	err = dataRepo.RevokeAccess(r.Context(), id, instanceID)
	if err != nil {
		msg := fmt.Sprintf("failed to revoke access to data repository for dataset %s: %s", id, err)
		handleError(w, apierror.New(apierror.ErrInternalError, msg, err))
		return
	}

	// write to audit log
	auditLog := service.AuditLogRepository.Log(r.Context(), group, id)
	msg := fmt.Sprintf("Revoked instance access to dataset %s (InstanceID: %s)", id, instanceID)
	auditLog <- msg

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNoContent)
	w.Write([]byte{})
}
