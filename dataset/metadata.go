package dataset

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"time"

	log "github.com/sirupsen/logrus"
)

// Metadata is the structure of dataset metadata
type Metadata struct {
	ID                  string     `json:"id"`
	Name                string     `json:"name"`
	Description         string     `json:"description"`
	CreatedAt           *time.Time `json:"created_at"`
	CreatedBy           string     `json:"created_by"`
	DataClassifications []string   `json:"data_classifications"`
	DataFormat          string     `json:"data_format"`
	DataStorage         string     `json:"data_storage"`
	Derivative          bool       `json:"derivative"`
	DuaURL              *url.URL   `json:"dua_url"`
	FinalizedAt         *time.Time `json:"finalized_at"`
	FinalizedBy         string     `json:"finalized_by"`
	ModifiedAt          *time.Time `json:"modified_at"`
	ModifiedBy          string     `json:"modified_by"`
	ProctorResponseURL  *url.URL   `json:"proctor_response_url"`
	SourceIDs           []string   `json:"source_ids"`
}

// UnmarshalJSON is a custom JSON unmarshaller for metadata
func (m *Metadata) UnmarshalJSON(j []byte) error {
	var rawStrings map[string]interface{}

	log.Debugf("unmarshalling metadata: %s", string(j))

	err := json.Unmarshal(j, &rawStrings)
	if err != nil {
		return err
	}

	log.Debug("unmarshaled metadata into rawstrings")

	if id, ok := rawStrings["id"]; ok {
		s, ok := id.(string)
		if !ok {
			msg := fmt.Sprintf("id is not a string: %+v", rawStrings["id"])
			return errors.New(msg)
		}
		m.ID = s
	}

	if name, ok := rawStrings["name"]; ok {
		s, ok := name.(string)
		if !ok {
			msg := fmt.Sprintf("name is not a string: %+v", rawStrings["name"])
			return errors.New(msg)
		}
		m.Name = s
	}

	if desc, ok := rawStrings["description"]; ok {
		s, ok := desc.(string)
		if !ok {
			msg := fmt.Sprintf("description is not a string: %+v", rawStrings["description"])
			return errors.New(msg)
		}
		m.Description = s
	}

	if createdAt, ok := rawStrings["created_at"]; ok {
		ca, ok := createdAt.(string)
		if !ok {
			msg := fmt.Sprintf("created_at is not a string: %+v", rawStrings["created_at"])
			return errors.New(msg)
		}
		if ca != "" {
			t, err := time.Parse(time.RFC3339, ca)
			if err != nil {
				msg := fmt.Sprintf("failed to parse created at as time: %+v", t)
				return errors.New(msg)
			}
			m.CreatedAt = &t
		}
	}

	if createdBy, ok := rawStrings["created_by"]; ok {
		s, ok := createdBy.(string)
		if !ok {
			msg := fmt.Sprintf("created_by is not a string: %+v", rawStrings["created_by"])
			return errors.New(msg)
		}
		m.CreatedBy = s
	}

	if dataClassifications, ok := rawStrings["data_classifications"]; ok {
		dcs, ok := dataClassifications.([]interface{})
		if !ok {
			msg := fmt.Sprintf("data_classification at is not a []interface{}: %+v", rawStrings["data_classifications"])
			return errors.New(msg)
		}
		m.DataClassifications = []string{}
		for _, iface := range dcs {
			dc, ok := iface.(string)
			if !ok {
				msg := fmt.Sprintf("data classification value is not a string: %+v", iface)
				return errors.New(msg)
			}
			m.DataClassifications = append(m.DataClassifications, dc)
		}
	}

	if dataFormat, ok := rawStrings["data_format"]; ok {
		s, ok := dataFormat.(string)
		if !ok {
			msg := fmt.Sprintf("data_format is not a string: %+v", rawStrings["data_format"])
			return errors.New(msg)
		}
		m.DataFormat = s
	}

	if dataStorage, ok := rawStrings["data_storage"]; ok {
		s, ok := dataStorage.(string)
		if !ok {
			msg := fmt.Sprintf("data_storage is not a string: %+v", rawStrings["data_storage"])
			return errors.New(msg)
		}
		m.DataStorage = s
	}

	if derivative, ok := rawStrings["derivative"]; ok {
		b, ok := derivative.(bool)
		if !ok {
			msg := fmt.Sprintf("derivative is not a boolean: %+v", rawStrings["derivative"])
			return errors.New(msg)
		}
		m.Derivative = b
	}

	if duaUrl, ok := rawStrings["dua_url"]; ok {
		d, ok := duaUrl.(string)
		if !ok {
			msg := fmt.Sprintf("dua url is not a string: %+v", rawStrings["dua_url"])
			return errors.New(msg)
		}
		u, err := url.Parse(d)
		if err != nil {
			msg := fmt.Sprintf("failed to parse dua url at as url: %+v", rawStrings["dua_url"])
			return errors.New(msg)
		}
		m.DuaURL = u
	}

	if finalizedAt, ok := rawStrings["finalized_at"]; ok {
		fa, ok := finalizedAt.(string)
		if !ok {
			msg := fmt.Sprintf("finalized_at is not a string: %+v", rawStrings["finalized_at"])
			return errors.New(msg)
		}
		if fa != "" {
			t, err := time.Parse(time.RFC3339, fa)
			if err != nil {
				msg := fmt.Sprintf("failed to parse finalized_at as time: %+v", t)
				return errors.New(msg)
			}
			m.FinalizedAt = &t
		}
	}

	if finalizedBy, ok := rawStrings["finalized_by"]; ok {
		s, ok := finalizedBy.(string)
		if !ok {
			msg := fmt.Sprintf("finalized_by is not a string: %+v", rawStrings["finalized_by"])
			return errors.New(msg)
		}
		m.FinalizedBy = s
	}

	if modifiedAt, ok := rawStrings["modified_at"]; ok {
		ma, ok := modifiedAt.(string)
		if !ok {
			msg := fmt.Sprintf("modified_at is not a string: %+v", rawStrings["modified_at"])
			return errors.New(msg)
		}
		if ma != "" {
			t, err := time.Parse(time.RFC3339, ma)
			if err != nil {
				msg := fmt.Sprintf("failed to parse modified_at as time: %+v", t)
				return errors.New(msg)
			}
			m.ModifiedAt = &t
		}
	}

	if modifiedBy, ok := rawStrings["modified_by"]; ok {
		s, ok := modifiedBy.(string)
		if !ok {
			msg := fmt.Sprintf("modified_by is not a string: %+v", rawStrings["modified_by"])
			return errors.New(msg)
		}
		m.ModifiedBy = s
	}

	if proctorResponseURL, ok := rawStrings["proctor_response_url"]; ok {
		p, ok := proctorResponseURL.(string)
		if !ok {
			msg := fmt.Sprintf("proctor_response_url is not a string: %+v", rawStrings["proctor_response_url"])
			return errors.New(msg)
		}
		u, err := url.Parse(p)
		if err != nil {
			msg := fmt.Sprintf("failed to parse proctor_response_url at as url: %+v", rawStrings["proctor_response_url"])
			return errors.New(msg)
		}
		m.ProctorResponseURL = u
	}

	if sourceIds, ok := rawStrings["source_ids"]; ok {
		if sourceIds == nil {
			m.SourceIDs = []string{}
		} else {
			sids, ok := sourceIds.([]interface{})
			if !ok {
				msg := fmt.Sprintf("source_ids at is not a []interface{}: %+v", rawStrings["source_ids"])
				return errors.New(msg)
			}
			m.SourceIDs = []string{}
			for _, iface := range sids {
				sid, ok := iface.(string)
				if !ok {
					msg := fmt.Sprintf("source id value is not a string: %+v", iface)
					return errors.New(msg)
				}
				m.SourceIDs = append(m.SourceIDs, sid)
			}
		}
	}

	return nil
}

// MarshalJSON is a custom JSON marshaller for metadata
func (m Metadata) MarshalJSON() ([]byte, error) {
	createdAt := ""
	if m.CreatedAt != nil {
		createdAt = m.CreatedAt.Format(time.RFC3339)
	}

	duaURL := ""
	if m.DuaURL != nil {
		duaURL = m.DuaURL.String()
	}

	finalizedAt := ""
	if m.FinalizedAt != nil {
		finalizedAt = m.FinalizedAt.Format(time.RFC3339)
	}

	modifiedAt := ""
	if m.ModifiedAt != nil {
		modifiedAt = m.ModifiedAt.Format(time.RFC3339)
	}

	proctorResponseURL := ""
	if m.ProctorResponseURL != nil {
		proctorResponseURL = m.ProctorResponseURL.String()
	}

	metadata := struct {
		ID                  string   `json:"id"`
		Name                string   `json:"name"`
		Description         string   `json:"description"`
		CreatedAt           string   `json:"created_at"`
		CreatedBy           string   `json:"created_by"`
		DataClassifications []string `json:"data_classifications"`
		DataFormat          string   `json:"data_format"`
		DataStorage         string   `json:"data_storage"`
		Derivative          bool     `json:"derivative"`
		DuaURL              string   `json:"dua_url"`
		FinalizedAt         string   `json:"finalized_at"`
		FinalizedBy         string   `json:"finalized_by"`
		ModifiedAt          string   `json:"modified_at"`
		ModifiedBy          string   `json:"modified_by"`
		ProctorResponseURL  string   `json:"proctor_response_url"`
		SourceIDs           []string `json:"source_ids"`
	}{
		ID:                  m.ID,
		Name:                m.Name,
		Description:         m.Description,
		CreatedAt:           createdAt,
		CreatedBy:           m.CreatedBy,
		DataClassifications: m.DataClassifications,
		DataFormat:          m.DataFormat,
		DataStorage:         m.DataStorage,
		Derivative:          m.Derivative,
		DuaURL:              duaURL,
		FinalizedAt:         finalizedAt,
		FinalizedBy:         m.FinalizedBy,
		ModifiedAt:          modifiedAt,
		ModifiedBy:          m.ModifiedBy,
		ProctorResponseURL:  proctorResponseURL,
		SourceIDs:           m.SourceIDs,
	}

	return json.Marshal(metadata)
}
