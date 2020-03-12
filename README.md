# ds-api

[![CircleCI](https://circleci.com/gh/YaleSpinup/ds-api.svg?style=svg)](https://circleci.com/gh/YaleSpinup/ds-api)

This API provides API access to the Spinup Data Set service.

## Endpoints

```
GET /v1/ds/ping
GET /v1/ds/version
GET /v1/ds/metrics

POST /v1/ds/{account}/datasets
```

## Usage

### Create a dataset

POST /v1/ds/{account}/datasets

```json
{
    "name": "awesome-dataset-of-stuff",
    "type": "s3",
    "derivative": true,
    "tags": [
        { "key": "Application", "value": "ButWhyyyyy" },
        { "key": "COA", "value": "Take.My.Money" },
        { "key": "CreatedBy", "value": "SomeGuy" }
    ],
    "metadata": {
        "description": "The hugest dataset of awesome stuff",
        "created_at": "2018-03-28T07:36:01.123Z",
        "created_by": "drzoidberg",
        "data_classifications": ["hipaa","pii"],
        "data_format": "file",
        "dua_url": "https://allmydata.s3.amazonaws.com/duas/huge_awesome_dua.pdf",
        "modified_at": "2019-03-28T07:36:01.123Z",
        "modified_by": "pfry",
        "proctor_response_url": "https://allmydata.s3.amazonaws.com/proctor/huge_awesome_study.json",
        "source_ids": ["e15d2282-9c68-46b5-801c-2b5a62484624", "a7c082ee-f711-48fa-8a57-25c95b3a6ddd"]
    }
}
```

#### Response

```json
{
    "id": "d37b375b-d136-4b17-8666-5036dc554a66",
    "repository": "dataset-localdev-d37b375b-d136-4b17-8666-5036dc554a66",
    "metadata": {
        "id": "d37b375b-d136-4b17-8666-5036dc554a66",
        "name": "awesome-dataset-of-stuff",
        "description": "The hugest dataset of awesome stuff",
        "created_at": "2020-03-11T18:41:32Z",
        "created_by": "drzoidberg",
        "data_classifications": [
            "hipaa",
            "pii"
        ],
        "data_format": "file",
        "data_storage": "s3",
        "derivative": true,
        "dua_url": "https://allmydata.s3.amazonaws.com/duas/huge_awesome_dua.pdf",
        "modified_at": "2020-03-11T18:41:32Z",
        "modified_by": "pfry",
        "proctor_response_url": "https://allmydata.s3.amazonaws.com/proctor/huge_awesome_study.json",
        "source_ids": [
            "e15d2282-9c68-46b5-801c-2b5a62484624",
            "a7c082ee-f711-48fa-8a57-25c95b3a6ddd"
        ]
    },
    "access": {
        "instance_profile_arn": "arn:aws:iam::516855177326:instance-profile/roleDataset_d37b375b-d136-4b17-8666-5036dc554a66",
        "instance_profile_name": "roleDataset_d37b375b-d136-4b17-8666-5036dc554a66",
        "policy_arn": "arn:aws:iam::516855177326:policy/dataset-localdev-d37b375b-d136-4b17-8666-5036dc554a66-DerivativePlc",
        "policy_name": "dataset-localdev-d37b375b-d136-4b17-8666-5036dc554a66-DerivativePlc",
        "role_arn": "arn:aws:iam::516855177326:role/roleDataset_d37b375b-d136-4b17-8666-5036dc554a66",
        "role_name": "roleDataset_d37b375b-d136-4b17-8666-5036dc554a66"
    }
}
```

| Response Code                 | Definition                           |
| ----------------------------- | -------------------------------------|
| **202 Accepted**              | creation request accepted            |
| **400 Bad Request**           | badly formed request                 |
| **403 Forbidden**             | you don't have access to bucket      |
| **404 Not Found**             | account not found                    |
| **409 Conflict**              | bucket or iam policy  already exists |
| **429 Too Many Requests**     | service or rate limit exceeded       |
| **500 Internal Server Error** | a server error occurred              |
| **503 Service Unavailable**   | an AWS service is unavailable        |

## Authentication

Authentication is accomplished using a pre-shared key via the `X-Auth-Token` header.

## Authors

E Camden Fisher <camden.fisher@yale.edu>
Tenyo Grozev <tenyo.grozev@yale.edu>

## License

GNU Affero General Public License v3.0 (GNU AGPLv3)  
Copyright (c) 2020 Yale University
