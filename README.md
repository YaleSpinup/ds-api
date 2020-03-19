# ds-api

[![CircleCI](https://circleci.com/gh/YaleSpinup/ds-api.svg?style=svg)](https://circleci.com/gh/YaleSpinup/ds-api)

This API provides API access to the Spinup Data Set service.

## Endpoints

```
GET /v1/ds/ping
GET /v1/ds/version
GET /v1/ds/metrics

POST /v1/ds/{account}/datasets
GET /v1/ds/{account}/datasets/{id}
DELETE /v1/ds/{account}/datasets/{id}
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

### Get information about a dataset

GET /v1/ds/{account}/datasets/{id}

```json
{
    "id": "bb4f6316-53e2-45ae-97c7-fa7fd17f78a8",
    "metadata": {
        "id": "bb4f6316-53e2-45ae-97c7-fa7fd17f78a8",
        "name": "awesome-dataset-of-stuff",
        "description": "The hugest dataset of awesome stuff",
        "created_at": "2020-03-16T15:38:14Z",
        "created_by": "drzoidberg",
        "data_classifications": [
            "hipaa",
            "pii"
        ],
        "data_format": "file",
        "data_storage": "s3",
        "derivative": true,
        "dua_url": "https://allmydata.s3.amazonaws.com/duas/huge_awesome_dua.pdf",
        "modified_at": "2020-03-16T15:38:14Z",
        "modified_by": "pfry",
        "proctor_response_url": "https://allmydata.s3.amazonaws.com/proctor/huge_awesome_study.json",
        "source_ids": [
            "d37b375b-d136-4b17-8666-5036dc554a66",
        ]
    },
    "repository": {
        "name": "dataset-localdev-bb4f6316-53e2-45ae-97c7-fa7fd17f78a8",
        "tags": [
            {
                "key": "CreatedBy",
                "value": "SomeGuy"
            },
            {
                "key": "spinup:org",
                "value": "localdev"
            },
            {
                "key": "ID",
                "value": "bb4f6316-53e2-45ae-97c7-fa7fd17f78a8"
            },
            {
                "key": "COA",
                "value": "Take.My.Money"
            },
            {
                "key": "Application",
                "value": "ButWhyyyyy"
            },
            {
                "key": "Name",
                "value": "awesome-dataset-of-stuff"
            }
        ]
    }
}
```

| Response Code                 | Definition                           |
| ----------------------------- | -------------------------------------|
| **200 OK**                    | okay                                 |
| **400 Bad Request**           | badly formed request                 |
| **404 Not Found**             | dataset not found                    |
| **500 Internal Server Error** | a server error occurred              |

### Delete a dataset

DELETE /v1/ds/{account}/datasets/{id}

| Response Code                 | Definition                           |
| ----------------------------- | -------------------------------------|
| **204 OK**                    | okay                                 |
| **400 Bad Request**           | badly formed request                 |
| **404 Not Found**             | dataset not found                    |
| **500 Internal Server Error** | a server error occurred              |


## Authentication

Authentication is accomplished using a pre-shared key via the `X-Auth-Token` header.

## API Configuration

API configuration is via `config/config.json`, an example config file is provided.

You can specify a single `metadataRepository` where metadata about all the different data sets will be stored. Currently, the only supported type is `s3`, so you need to provide an S3 bucket and credentials with full access to that bucket. For example, if you created a bucket called `spinup-example-metadata-repository`, then the IAM policy would be:
```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "s3:*",
            "Resource": [
                "arn:aws:s3:::spinup-example-metadata-repository",
                "arn:aws:s3:::spinup-example-metadata-repository/*"
            ]
        }
    ]
}
```

You can then define a list of `accounts` for the actual dataset repositories - that's where the data sets will be stored. Currently, the only supported type is `s3`, so you need to provide credentials in each account with the appropriate S3 and IAM access. This is a good starting IAM policy if you don't modify the default name and path prefixes:
```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "iam:*",
            "Resource": [
                "arn:aws:iam::*:role/spinup/dataset/*",
                "arn:aws:iam::*:instance-profile/spinup/dataset/*",
                "arn:aws:iam::*:group/spinup/dataset/*",
                "arn:aws:iam::*:user/spinup/dataset/*",
                "arn:aws:iam::*:policy/spinup/dataset/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": "s3:*",
            "Resource": [
                "arn:aws:s3::*:dataset-*"
            ]
        }
    ]
}
```

## Authors

E Camden Fisher <camden.fisher@yale.edu>
Tenyo Grozev <tenyo.grozev@yale.edu>

## License

GNU Affero General Public License v3.0 (GNU AGPLv3)  
Copyright (c) 2020 Yale University
