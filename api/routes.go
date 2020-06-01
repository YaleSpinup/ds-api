package api

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func (s *server) routes() {
	api := s.router.PathPrefix("/v1/ds").Subrouter()
	api.HandleFunc("/ping", s.PingHandler).Methods(http.MethodGet)
	api.HandleFunc("/version", s.VersionHandler).Methods(http.MethodGet)
	api.Handle("/metrics", promhttp.Handler()).Methods(http.MethodGet)

	api.HandleFunc("/{account}/datasets", s.DatasetListHandler).Methods(http.MethodGet)
	api.HandleFunc("/{account}/datasets", s.DatasetCreateHandler).Methods(http.MethodPost)
	api.HandleFunc("/{account}/datasets/{id}", s.DatasetShowHandler).Methods(http.MethodGet)
	api.HandleFunc("/{account}/datasets/{id}", s.DatasetPromoteHandler).Methods(http.MethodPatch)
	api.HandleFunc("/{account}/datasets/{id}", s.DatasetUpdateHandler).Methods(http.MethodPut)
	api.HandleFunc("/{account}/datasets/{id}", s.DatasetDeleteHandler).Methods(http.MethodDelete)

	api.HandleFunc("/{account}/datasets/{id}/attachments", s.AttachmentListHandler).Methods(http.MethodGet)
	api.HandleFunc("/{account}/datasets/{id}/attachments", s.AttachmentCreateHandler).Methods(http.MethodPost)
	api.HandleFunc("/{account}/datasets/{id}/attachments", s.AttachmentDeleteHandler).Methods(http.MethodDelete)

	api.HandleFunc("/{account}/datasets/{id}/instances", s.InstanceListHandler).Methods(http.MethodGet)
	api.HandleFunc("/{account}/datasets/{id}/instances", s.InstanceCreateHandler).Methods(http.MethodPost)
	api.HandleFunc("/{account}/datasets/{id}/instances/{instance_id}", s.InstanceDeleteHandler).Methods(http.MethodDelete)

	api.HandleFunc("/{account}/datasets/{id}/users", s.UserListHandler).Methods(http.MethodGet)
	api.HandleFunc("/{account}/datasets/{id}/users", s.UserCreateHandler).Methods(http.MethodPost)
	api.HandleFunc("/{account}/datasets/{id}/users", s.UserDeleteHandler).Methods(http.MethodDelete)
	api.HandleFunc("/{account}/datasets/{id}/users", s.UserUpdateHandler).Methods(http.MethodPut)
}
