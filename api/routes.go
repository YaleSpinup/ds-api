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
	api.HandleFunc("/{account}/datasets/{id}", s.DatasetUpdateHandler).Methods(http.MethodPut)
	api.HandleFunc("/{account}/datasets/{id}", s.DatasetDeleteHandler).Methods(http.MethodDelete)

}
