package common

import (
	"bytes"
	"reflect"
	"testing"
)

var testConfig = []byte(
	`{
		"listenAddress": ":8000",
		"metadataRepository": {
			"type": "s3",
			"config": {
			  "bucket": "some-metadata-repository",
			  "region": "us-east-1",
			  "akid": "keymeta",
			  "secret": "secretmeta",
			  "endpoint": "https://s3.horseradishsys.com",
			  "awesome": true
			}
		},
		"accounts": {
		  "provider1": {
				"storageProviders": ["s3"],
				"config": {
					"region": "us-east-1",
					"akid": "key1",
					"secret": "secret1",
					"loggingBucket": "dsapi-provider1-access-logs"
				}
		  },
		  "provider2": {
				"storageProviders": ["s3"],
				"config": {
					"region": "us-west-1",
					"akid": "key2",
					"secret": "secret2",
					"loggingBucket": "dsapi-provider2-access-logs"
				}
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
				StorageProviders: []string{"s3"},
				Config: map[string]interface{}{
					"region":        "us-east-1",
					"akid":          "key1",
					"secret":        "secret1",
					"loggingBucket": "dsapi-provider1-access-logs",
				},
			},
			"provider2": Account{
				StorageProviders: []string{"s3"},
				Config: map[string]interface{}{
					"region":        "us-west-1",
					"akid":          "key2",
					"secret":        "secret2",
					"loggingBucket": "dsapi-provider2-access-logs",
				},
			},
		},
		MetadataRepository: MetadataRepository{
			Type: "s3",
			Config: map[string]interface{}{
				"bucket":   "some-metadata-repository",
				"region":   "us-east-1",
				"akid":     "keymeta",
				"secret":   "secretmeta",
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
