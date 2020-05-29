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

POST /v1/ds/{account}/datasets/{id}/attachments
DELETE /v1/ds/{account}/datasets/{id}/attachments
GET /v1/ds/{account}/datasets/{id}/attachments

GET /v1/ds/{account}/datasets/{id}/instances
POST /v1/ds/{account}/datasets/{id}/instances
DELETE /v1/ds/{account}/datasets/{id}/instances/{instance_id}

GET /v1/ds/{account}/datasets/{id}/users
POST /v1/ds/{account}/datasets/{id}/users
DELETE /v1/ds/{account}/datasets/{id}/users
PUT /v1/ds/{account}/datasets/{id}/users
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
        "empty": false,
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


### Create attachment for a dataset

POST /v1/ds/{account}/datasets/{id}/attachments

The request needs to be a `multipart/form-data` with the following parameters:
  - `name` - the name of the attachment as it should be saved, e.g. `eula.txt`
  - `attachment` - the content of the file being uploaded

#### Response

```json
[
    "eula.txt"
]
```

| Response Code                 | Definition                           |
| ----------------------------- | -------------------------------------|
| **200 OK**                    | okay                                 |
| **400 Bad Request**           | badly formed request, or file too big|
| **404 Not Found**             | dataset not found                    |
| **500 Internal Server Error** | a server error occurred              |

### Delete attachment from a dataset

DELETE /v1/ds/{account}/datasets/{id}/attachments

```json
{
	"attachment_name": "dummy.doc"
}
```

#### Response

| Response Code                 | Definition                                   |
| ----------------------------- | ---------------------------------------------|
| **204 OK**                    | attachment deleted, if it existed            |
| **400 Bad Request**           | bad request                                  |
| **404 Not Found**             | account/dataset not found                    |
| **500 Internal Server Error** | a server error occurred                      |


### Get attachments for a dataset

GET /v1/ds/{account}/datasets/{id}/attachments

#### Response

```json
[
    {
        "Name": "Dataset Data Use Agreement.pdf",
        "Modified": "2020-05-17T02:04:27Z",
        "Size": 3708454,
        "URL": "https://dataset-localdev-3cadbe31-27e9-4f7a-9515-51ec9d754022.s3.amazonaws.com/_attachments/Dataset%20Data%20Use%20Agreement.pdf?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAXQVXYEBXA5X5LRN3%2F20200518%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20200518T132423Z&X-Amz-Expires=300&X-Amz-SignedHeaders=host&X-Amz-Signature=342d937b7b726408c2efe41493d126ea577204f85ffe77ffc9b3cf22af80c7ea"
    },
    {
        "Name": "eula.txt",
        "Modified": "2020-05-18T13:19:34Z",
        "Size": 6920,
        "URL": "https://dataset-localdev-3cadbe31-27e9-4f7a-9515-51ec9d754022.s3.amazonaws.com/_attachments/eula.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAXQVXYEBXA5X5LRN3%2F20200518%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20200518T132423Z&X-Amz-Expires=300&X-Amz-SignedHeaders=host&X-Amz-Signature=c2d7f7165ce3c099e8eefcb14e3b4c7e0e6a319af48d6727f25519f35488b14a"
    }
]
```

| Response Code                 | Definition                           |
| ----------------------------- | -------------------------------------|
| **200 OK**                    | okay                                 |
| **400 Bad Request**           | badly formed request                 |
| **404 Not Found**             | account/dataset not found            |
| **500 Internal Server Error** | a server error occurred              |


### List all instances that have access to a dataset

GET /v1/ds/{account}/datasets/{id}/instances

```json
{
    "id": "95db5a7b-466b-4aa7-bbe1-1e23ed860f32",
    "access": {
        "i-01f9bfb7ee683e807": "instanceRole_i-01f9bfb7ee683e807"
    }
}
```

| Response Code                 | Definition                           |
| ----------------------------- | -------------------------------------|
| **200 OK**                    | okay                                 |
| **400 Bad Request**           | badly formed request                 |
| **404 Not Found**             | account/dataset not found            |
| **500 Internal Server Error** | a server error occurred              |

### Grant dataset access to an instance

POST /v1/ds/{account}/datasets/{id}/instances

```json
{
	"instance_id": "i-01f9bfb7ee683e807"
}
```

#### Response

```json
{
    "id": "95db5a7b-466b-4aa7-bbe1-1e23ed860f32",
    "access": {
        "i-01f9bfb7ee683e807": "instanceRole_i-01f9bfb7ee683e807"
    }
}
```

| Response Code                 | Definition                           |
| ----------------------------- | -------------------------------------|
| **200 OK**                    | instance access granted              |
| **400 Bad Request**           | badly formed request                 |
| **404 Not Found**             | account/dataset not found            |
| **500 Internal Server Error** | a server error occurred              |

### Revoke dataset access from an instance

DELETE /v1/ds/{account}/datasets/{id}/instances/{instance_id}

| Response Code                 | Definition                                   |
| ----------------------------- | ---------------------------------------------|
| **204 OK**                    | instance access revoked                      |
| **400 Bad Request**           | bad request, or instance doesn't have access |
| **404 Not Found**             | account/dataset not found                    |
| **500 Internal Server Error** | a server error occurred                      |

### Create a user for a dataset

POST /v1/ds/{account}/datasets/{id}/users

Request body is empty.

#### Response

```json
{
    "user": "dataset-ssdev-95db5a7b-466b-4aa7-bbe1-1e23ed860f32-DsTmpUsr",
    "group": "dataset-ssdev-95db5a7b-466b-4aa7-bbe1-1e23ed860f32-DsTmpGrp",
    "policy": "dataset-ssdev-95db5a7b-466b-4aa7-bbe1-1e23ed860f32-DsTmpPlc",
    "credentials": {
        "akid": "XXXXXXXXXXXXXXXXXXXX",
        "secret": "secretsecretsecretsecretsecretsecret",
    }
}
```

| Response Code                 | Definition                           |
| ----------------------------- | -------------------------------------|
| **200 OK**                    | instance access granted              |
| **400 Bad Request**           | badly formed request                 |
| **404 Not Found**             | account/dataset not found            |
| **409 Conflict**              | user already exists                  |
| **500 Internal Server Error** | a server error occurred              |


### Delete a user for a dataset

DELETE /v1/ds/{account}/datasets/{id}/users

#### Response

| Response Code                 | Definition                           |
| ----------------------------- | -------------------------------------|
| **200 OK**                    | instance access granted              |
| **400 Bad Request**           | badly formed request                 |
| **404 Not Found**             | account/dataset/user not found       |
| **500 Internal Server Error** | a server error occurred              |

### Get a user for a dataset

GET /v1/ds/{account}/datasets/{id}/users

#### Response

```json
{
    "dataset-ssdev-95db5a7b-466b-4aa7-bbe1-1e23ed860f32-DsTmpUsr": {
        "keys": {
            "XXXXXXXXXXXXXXXXXXXX": "Inactive",
            "YYYYYYYYYYYYYYYYYYYY": "Active"
        }
    }
}
```

| Response Code                 | Definition                           |
| ----------------------------- | -------------------------------------|
| **200 OK**                    | instance access granted              |
| **400 Bad Request**           | badly formed request                 |
| **404 Not Found**             | account/dataset/user not found       |
| **500 Internal Server Error** | a server error occurred              |

### Update a user's key for a dataset

PUT /v1/ds/{account}/datasets/{id}/users

Request body is empty.

#### Response

```json
{
    "keys": {
        "XXXXXXXXXXXXXXXXXXXXX": "Inactive"
    },
    "credentials": {
        "akid": "YYYYYYYYYYYYYYYYYYYYY",
        "secret": "secretsecretsecretsecretsecretsecret"
    }
}
```

| Response Code                 | Definition                           |
| ----------------------------- | -------------------------------------|
| **200 OK**                    | instance access granted              |
| **400 Bad Request**           | badly formed request                 |
| **404 Not Found**             | account/dataset not found            |
| **429 Limit Exceeded**        | maximum number of keys               |
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
            "Action": [
                "iam:GetRole",
                "iam:GetInstanceProfile",
                "iam:ListAttachedRolePolicies",
                "iam:PassRole"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": "s3:*",
            "Resource": [
                "arn:aws:s3::*:dataset-*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "ec2:AssociateIamInstanceProfile",
                "ec2:DescribeIamInstanceProfileAssociations",
                "ec2:DescribeInstances",
                "ec2:DisassociateIamInstanceProfile"
            ],
            "Resource": "*"
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
