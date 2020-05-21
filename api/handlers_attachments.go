package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/YaleSpinup/ds-api/apierror"
	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
)

// limit individual attachment files to 32 MB
const maxAttachmentSize = 32 * 1024 * 1024

// AttachmentCreateHandler adds an attachment file to a dataset
func (s *server) AttachmentCreateHandler(w http.ResponseWriter, r *http.Request) {
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

	metadata, err := service.MetadataRepository.Get(r.Context(), account, id)
	if err != nil {
		handleError(w, err)
		return
	}

	attachmentRepo, ok := service.AttachmentRepository[metadata.DataStorage]
	if !ok {
		msg := fmt.Sprintf("requested attachment repository type not supported for this account: %s", metadata.DataStorage)
		handleError(w, apierror.New(apierror.ErrBadRequest, msg, nil))
		return
	}

	// limit total request size to 50 MB
	r.Body = http.MaxBytesReader(w, r.Body, 50<<20)
	defer r.Body.Close()

	// parse the multipart form and keep up to 32 MB in memory (the rest on temp disk)
	err = r.ParseMultipartForm(32 << 20)
	if err != nil {
		handleError(w, apierror.New(apierror.ErrBadRequest, "failed to parse multipart request", err))
		return
	}

	// get the attachment name
	attachmentName := r.FormValue("name")
	if attachmentName == "" {
		handleError(w, apierror.New(apierror.ErrBadRequest, "failed to parse form value: name", err))
		return
	}

	log.Debugf("attachment name: %s", attachmentName)

	// get the (first) attachment file
	attachment, attachmentHeader, err := r.FormFile("attachment")
	if err != nil {
		handleError(w, apierror.New(apierror.ErrBadRequest, "failed to parse attachment", err))
		return
	}
	defer attachment.Close()

	log.Debugf("attachment size (bytes): %v", attachmentHeader.Size)

	if attachmentHeader.Size > maxAttachmentSize {
		msg := fmt.Sprintf("attachment size too big (max limit is %d bytes)", maxAttachmentSize)
		handleError(w, apierror.New(apierror.ErrBadRequest, msg, err))
		return
	}

	err = attachmentRepo.CreateAttachment(r.Context(), id, attachmentName, attachment)
	if err != nil {
		msg := fmt.Sprintf("failed to create attachment for dataset %s: %s", id, err)
		handleError(w, apierror.New(apierror.ErrInternalError, msg, err))
		return
	}

	output := []string{
		attachmentName,
	}

	j, err := json.Marshal(&output)
	if err != nil {
		msg := fmt.Sprintf("cannot encode dataset attachment output into json: %s", err)
		handleError(w, apierror.New(apierror.ErrBadRequest, msg, err))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(j)
}

// AttachmentListHandler lists all attachments for a dataset
func (s *server) AttachmentListHandler(w http.ResponseWriter, r *http.Request) {
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

	metadata, err := service.MetadataRepository.Get(r.Context(), account, id)
	if err != nil {
		handleError(w, err)
		return
	}

	attachmentRepo, ok := service.AttachmentRepository[metadata.DataStorage]
	if !ok {
		msg := fmt.Sprintf("requested attachment repository type not supported for this account: %s", metadata.DataStorage)
		handleError(w, apierror.New(apierror.ErrBadRequest, msg, nil))
		return
	}

	// list attachments for this data repository
	datasetAttachments, err := attachmentRepo.ListAttachments(r.Context(), id, true)
	if err != nil {
		msg := fmt.Sprintf("failed to list attachments for dataset %s: %s", id, err)
		handleError(w, apierror.New(apierror.ErrInternalError, msg, err))
		return
	}

	j, err := json.Marshal(&datasetAttachments)
	if err != nil {
		msg := fmt.Sprintf("cannot encode dataset attachments output into json: %s", err)
		handleError(w, apierror.New(apierror.ErrBadRequest, msg, err))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(j)
}

// AttachmentDeleteHandler removes an attachment file from a dataset
func (s *server) AttachmentDeleteHandler(w http.ResponseWriter, r *http.Request) {
	// TODO
}
