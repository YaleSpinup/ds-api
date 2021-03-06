package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/YaleSpinup/ds-api/apierror"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// UserListHandler lists the users for a dataset
func (s *server) UserListHandler(w http.ResponseWriter, r *http.Request) {
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

	log.Debugf("listing users of dataset '%s' in account %s", id, account)

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

	// list users of this data repository
	datasetUsers, err := dataRepo.ListUsers(r.Context(), id)
	if err != nil {
		// AWS returns a Forbidden code when getting a group that doesn't exist.  The alternative is to list *all*
		// of the groups in the prefix and see if the group exists first, but that seems slow and expensive.  We could
		// reverse the paradigm and list the groups for the expected user, but that will limit us if we support more
		// than one user later.  This seems like the best compromise since 1) we should never get Forbidden unless *we*
		// make a mistake. 2) we control the name of the group/user, etc.
		aerr, ok := errors.Cause(err).(apierror.Error)
		if !ok || aerr.Code != apierror.ErrForbidden {
			handleError(w, errors.Wrapf(err, "list users of data repository for dataset %s", id))
			return
		}
		datasetUsers = make(map[string]interface{})
	}

	j, err := json.Marshal(datasetUsers)
	if err != nil {
		log.Errorf("cannot marshal reasponse(%v) into JSON: %s", datasetUsers, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(j)
}

// UserCreateHandler creates a user for a dataset
func (s *server) UserCreateHandler(w http.ResponseWriter, r *http.Request) {
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

	log.Debugf("creating user of dataset '%s' in account %s", id, account)

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

	user, err := dataRepo.CreateUser(r.Context(), id)
	if err != nil {
		handleError(w, errors.Wrapf(err, "create user of data repository for dataset %s", id))
		return
	}

	j, err := json.Marshal(user)
	if err != nil {
		log.Errorf("cannot marshal reasponse(%v) into JSON: %s", user, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// write to audit log
	auditLog := service.AuditLogRepository.Log(r.Context(), group, id)
	msg := fmt.Sprintf("Created user with access to dataset %s", id)
	auditLog <- msg

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(j)
}

// UserDeleteHandler deletes a user of a dataset
func (s *server) UserDeleteHandler(w http.ResponseWriter, r *http.Request) {
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

	log.Debugf("deleting user of dataset '%s' in account %s", id, account)

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

	// This will return a "Forbidden" response if the group/user doesn't exist (bubbles up from AWS)
	err = dataRepo.DeleteUser(r.Context(), id)
	if err != nil {
		handleError(w, errors.Wrapf(err, "delete user of data repository for dataset %s", id))
		return
	}

	// write to audit log
	auditLog := service.AuditLogRepository.Log(r.Context(), group, id)
	msg := fmt.Sprintf("Deleted user with access to dataset %s", id)
	auditLog <- msg

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// UserUpdateHandler updates key for a user of a dataset
func (s *server) UserUpdateHandler(w http.ResponseWriter, r *http.Request) {
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

	log.Debugf("updating user of dataset '%s' in account %s", id, account)

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

	out, err := dataRepo.UpdateUser(r.Context(), id)
	if err != nil {
		handleError(w, errors.Wrapf(err, "update user of data repository for dataset %s", id))
		return
	}

	j, err := json.Marshal(out)
	if err != nil {
		log.Errorf("cannot marshal reasponse(%v) into JSON: %s", out, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// write to audit log
	auditLog := service.AuditLogRepository.Log(r.Context(), group, id)
	msg := fmt.Sprintf("Updated key for user with access to dataset %s", id)
	auditLog <- msg

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(j)
}
