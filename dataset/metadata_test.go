package dataset

import (
	"bytes"
	"net/url"
	"reflect"
	"testing"
	"time"
)

func TestMetadataUnmarshalJSON(t *testing.T) {
	var rawMetadata = []byte(`
	{
		"id": "08d754ba-8540-4fdc-92f3-47950c1cdb1c",
		"name": "alien-sightings-dataset",
		"description": "Alien sightings",
		"created_at": "2013-06-19T19:14:01.123Z",
		"created_by": "zbrannigan",
		"data_classifications": ["extremelyclassified"],
		"data_format": "file",
		"data_storage": "s3",
		"derivative": false,
		"dua_url": "https://allmydata.s3.amazonaws.com/duas/alien_dua.pdf",
		"modified_at": "2015-11-21T04:19:01.123Z",
		"modified_by": "kkroker",
		"proctor_response_url": "https://allmydata.s3.amazonaws.com/proctor/alien_study.json",
		"source_ids": [
			"ea19d935-6ca3-4711-8e3e-24713cc3ac00",
			"801e1c4f-58ff-4f14-af1f-0fd6a09cdaef",
			"c00925d6-2eef-4fb6-aef1-87152613222c"
		]
	}`)

	var createdAt, _ = time.Parse(time.RFC3339, "2013-06-19T19:14:01.123Z")
	var modifiedAt, _ = time.Parse(time.RFC3339, "2015-11-21T04:19:01.123Z")
	var duaURL, _ = url.Parse("https://allmydata.s3.amazonaws.com/duas/alien_dua.pdf")
	var procURL, _ = url.Parse("https://allmydata.s3.amazonaws.com/proctor/alien_study.json")
	var testMetadata = &Metadata{
		ID:                  "08d754ba-8540-4fdc-92f3-47950c1cdb1c",
		Name:                "alien-sightings-dataset",
		Description:         "Alien sightings",
		CreatedAt:           &createdAt,
		CreatedBy:           "zbrannigan",
		DataClassifications: []string{"extremelyclassified"},
		DataFormat:          "file",
		DataStorage:         "s3",
		Derivative:          false,
		DuaURL:              duaURL,
		ModifiedAt:          &modifiedAt,
		ModifiedBy:          "kkroker",
		ProctorResponseURL:  procURL,
		SourceIDs: []string{
			"ea19d935-6ca3-4711-8e3e-24713cc3ac00",
			"801e1c4f-58ff-4f14-af1f-0fd6a09cdaef",
			"c00925d6-2eef-4fb6-aef1-87152613222c",
		},
	}

	out := &Metadata{}
	err := out.UnmarshalJSON(rawMetadata)
	if err != nil {
		t.Errorf("expected nil error, got %s", err)
	}

	if !reflect.DeepEqual(out, testMetadata) {
		t.Errorf("expected: %+v,\n got %+v\n", testMetadata, out)
	}

	// bad json
	if err := out.UnmarshalJSON([]byte("{")); err == nil {
		t.Error("expected error for bad json, got nil")
	}

	// id type
	if err := out.UnmarshalJSON([]byte(`{"id":false}`)); err == nil {
		t.Error("expected error for bad id, got nil")
	}

	// name type
	if err := out.UnmarshalJSON([]byte(`{"name":false}`)); err == nil {
		t.Error("expected error for bad name, got nil")
	}

	// description type
	if err := out.UnmarshalJSON([]byte(`{"description":false}`)); err == nil {
		t.Error("expected error for bad description, got nil")
	}

	// created_at type
	if err := out.UnmarshalJSON([]byte(`{"created_at":false}`)); err == nil {
		t.Error("expected error for bad created_at, got nil")
	}

	// created_at date
	if err := out.UnmarshalJSON([]byte(`{"created_at":"12345"}`)); err == nil {
		t.Error("expected error for bad created_at date, got nil")
	}

	// created_by type
	if err := out.UnmarshalJSON([]byte(`{"created_by":false}`)); err == nil {
		t.Error("expected error for bad created_by, got nil")
	}

	// data_classifications type
	if err := out.UnmarshalJSON([]byte(`{"data_classifications":false}`)); err == nil {
		t.Error("expected error for bad data_classifications, got nil")
	}

	// data_classifications array type
	if err := out.UnmarshalJSON([]byte(`{"data_classifications":[false,true,false]}`)); err == nil {
		t.Error("expected error for bad data_classifications array, got nil")
	}

	// data_format type
	if err := out.UnmarshalJSON([]byte(`{"data_format":false}`)); err == nil {
		t.Error("expected error for bad data_format, got nil")
	}

	// derivative type
	if err := out.UnmarshalJSON([]byte(`{"derivative":"false"}`)); err == nil {
		t.Error("expected error for bad derivative, got nil")
	}

	// dua_url type
	if err := out.UnmarshalJSON([]byte(`{"dua_url":false}`)); err == nil {
		t.Error("expected error for bad dua_url, got nil")
	}

	// modified_at type
	if err := out.UnmarshalJSON([]byte(`{"modified_at":false}`)); err == nil {
		t.Error("expected error for bad modified_at, got nil")
	}

	// modified_at date type
	if err := out.UnmarshalJSON([]byte(`{"modified_at":"12345"}`)); err == nil {
		t.Error("expected error for bad modified_at, got nil")
	}

	// modified_by type
	if err := out.UnmarshalJSON([]byte(`{"modified_by":false}`)); err == nil {
		t.Error("expected error for bad modified_by, got nil")
	}

	// proctor_response_url type
	if err := out.UnmarshalJSON([]byte(`{"proctor_response_url":false}`)); err == nil {
		t.Error("expected error for bad proctor_response_url, got nil")
	}

	// source_ids type
	if err := out.UnmarshalJSON([]byte(`{"source_ids":false}`)); err == nil {
		t.Error("expected error for bad source_ids, got nil")
	}

	// source_ids array type
	if err := out.UnmarshalJSON([]byte(`{"source_ids":[false,true,false]}`)); err == nil {
		t.Error("expected error for bad source_ids array, got nil")
	}

}

func TestMetadataMarshalJSON(t *testing.T) {
	type test struct {
		input  Metadata
		output []byte
		err    error
	}

	createdAt, _ := time.Parse(time.RFC3339, "2013-06-19T19:14:01.123Z")
	modifiedAt, _ := time.Parse(time.RFC3339, "2015-11-21T04:19:01.123Z")
	duaURL, _ := url.Parse("https://allmydata.s3.amazonaws.com/duas/alien_dua.pdf")
	procURL, _ := url.Parse("https://allmydata.s3.amazonaws.com/proctor/alien_study.json")

	tests := []test{
		test{
			Metadata{},
			[]byte(`{"id":"","name":"","description":"","created_at":"","created_by":"","data_classifications":null,"data_format":"","data_storage":"","derivative":false,"dua_url":"","modified_at":"","modified_by":"","proctor_response_url":"","source_id":null}`),
			nil,
		},
		test{
			Metadata{
				ID:                  "08d754ba-8540-4fdc-92f3-47950c1cdb1c",
				Name:                "alien-sightings-dataset",
				Description:         "Alien sightings",
				CreatedAt:           &createdAt,
				CreatedBy:           "zbrannigan",
				DataClassifications: []string{"extremelyclassified"},
				DataFormat:          "file",
				DataStorage:         "s3",
				Derivative:          false,
				DuaURL:              duaURL,
				ModifiedAt:          &modifiedAt,
				ModifiedBy:          "kkroker",
				ProctorResponseURL:  procURL,
				SourceIDs: []string{
					"ea19d935-6ca3-4711-8e3e-24713cc3ac00",
					"801e1c4f-58ff-4f14-af1f-0fd6a09cdaef",
					"c00925d6-2eef-4fb6-aef1-87152613222c",
				},
			},
			[]byte(`{"id":"08d754ba-8540-4fdc-92f3-47950c1cdb1c","name":"alien-sightings-dataset","description":"Alien sightings","created_at":"2013-06-19T19:14:01Z","created_by":"zbrannigan","data_classifications":["extremelyclassified"],"data_format":"file","data_storage":"s3","derivative":false,"dua_url":"https://allmydata.s3.amazonaws.com/duas/alien_dua.pdf","modified_at":"2015-11-21T04:19:01Z","modified_by":"kkroker","proctor_response_url":"https://allmydata.s3.amazonaws.com/proctor/alien_study.json","source_id":["ea19d935-6ca3-4711-8e3e-24713cc3ac00","801e1c4f-58ff-4f14-af1f-0fd6a09cdaef","c00925d6-2eef-4fb6-aef1-87152613222c"]}`),
			nil,
		},
	}

	for _, tst := range tests {
		out, err := tst.input.MarshalJSON()
		if tst.err == nil && err != nil {
			t.Errorf("expected nil error, got %s", err)
		} else if tst.err != nil && err == nil {
			t.Errorf("expected error '%s', got nil", tst.err)
		}

		if !bytes.Equal(out, tst.output) {
			t.Errorf("expected: %s, got %s", string(tst.output), string(out))
		}
	}
}
