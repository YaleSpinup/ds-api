package common

import (
	"encoding/json"
	"io"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// Config is representation of the configuration data
type Config struct {
	ListenAddress      string
	Accounts           map[string]Account
	MetadataRepository MetadataRepository
	Token              string
	LogLevel           string
	Version            Version
	Org                string
}

// Account is the configuration for an individual account
type Account struct {
	StorageProviders []string
	Config           map[string]interface{}
}

// MetadataRepository is the configuration for the metadata respository
type MetadataRepository struct {
	Type   string
	Config map[string]interface{}
}

// Version carries around the API version information
type Version struct {
	Version           string
	VersionPrerelease string
	BuildStamp        string
	GitHash           string
}

// ReadConfig decodes the configuration from an io Reader
func ReadConfig(r io.Reader) (Config, error) {
	var c Config
	log.Infoln("Reading configuration")
	if err := json.NewDecoder(r).Decode(&c); err != nil {
		return c, errors.Wrap(err, "unable to decode JSON message")
	}
	return c, nil
}
