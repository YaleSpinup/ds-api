package common

import (
	"bytes"
	"reflect"
	"testing"
)

var testConfig = []byte(
	`{
		"listenAddress": ":8000",
		"accounts": {
		  "provider1": {
			"region": "us-east-1",
			"akid": "key1",
			"secret": "secret1"
		  },
		  "provider2": {
			"region": "us-west-1",
			"akid": "key2",
			"secret": "secret2"
		  }
		},
		"repository": {
			"type": "s3",
			"config": {
			  "bucket": "some-metadata-repository",
			  "region": "us-east-1",
			  "akid": "key3",
			  "secret": "secret3",
			  "endpoint": "https://s3.horseradishsys.com",
			  "awesome": true
			}
		  },
		"token": "SEKRET",
		"logLevel": "info",
		"org": "test"
	}`)

var brokenConfig = []byte(`{ "foobar": { "baz": "biz" }`)

func TestReadConfig(t *testing.T) {
	expectedConfig := Config{
		ListenAddress: ":8000",
		Accounts: map[string]Account{
			"provider1": Account{
				Region: "us-east-1",
				Akid:   "key1",
				Secret: "secret1",
			},
			"provider2": Account{
				Region: "us-west-1",
				Akid:   "key2",
				Secret: "secret2",
			},
		},
		Repository: Repository{
			Type: "s3",
			Config: map[string]interface{}{
				"bucket":   "some-metadata-repository",
				"region":   "us-east-1",
				"akid":     "key3",
				"secret":   "secret3",
				"endpoint": "https://s3.horseradishsys.com",
				"awesome":  true,
			},
		},
		Token:    "SEKRET",
		LogLevel: "info",
		Org:      "test",
	}

	actualConfig, err := ReadConfig(bytes.NewReader(testConfig))
	if err != nil {
		t.Error("Failed to read config", err)
	}

	if !reflect.DeepEqual(actualConfig, expectedConfig) {
		t.Errorf("Expected config to be %+v\n got %+v", expectedConfig, actualConfig)
	}

	_, err = ReadConfig(bytes.NewReader(brokenConfig))
	if err == nil {
		t.Error("expected error reading config, got nil")
	}
}
