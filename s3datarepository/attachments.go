package s3datarepository

import (
	"context"
	"errors"
	"mime/multipart"
	"strings"
	"time"

	"github.com/YaleSpinup/ds-api/apierror"
	"github.com/YaleSpinup/ds-api/dataset"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	log "github.com/sirupsen/logrus"
)

const attachmentsPrefix = "_attachments/"

// CreateAttachment uploads a new attachment to the data repository
func (s *S3Repository) CreateAttachment(ctx context.Context, id, attachmentName string, attachmentBody multipart.File) error {
	if id == "" {
		return apierror.New(apierror.ErrBadRequest, "invalid input", errors.New("empty id"))
	}

	if attachmentName == "" {
		return apierror.New(apierror.ErrBadRequest, "invalid input", errors.New("empty attachment name"))
	}

	name := id
	if s.NamePrefix != "" {
		name = s.NamePrefix + "-" + name
	}

	attachmentName = attachmentsPrefix + attachmentName

	log.Debugf("uploading attachment '%s' to s3datarepository: %s", attachmentName, name)

	if _, err := s.S3Uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket: aws.String(name),
		Key:    aws.String(attachmentName),
		Body:   attachmentBody,
	}); err != nil {
		return ErrCode("failed to upload attachment to s3 bucket "+name, err)
	}

	return nil
}

// ListAttachments lists all attachments for the data repository
func (s *S3Repository) ListAttachments(ctx context.Context, id string) ([]dataset.Attachment, error) {
	if id == "" {
		return nil, apierror.New(apierror.ErrBadRequest, "invalid input", errors.New("empty id"))
	}

	name := id
	if s.NamePrefix != "" {
		name = s.NamePrefix + "-" + name
	}

	log.Debugf("getting list of attachments for s3datarepository: %s", name)

	return s.listAttachmentObjects(ctx, name, attachmentsPrefix)
}

// listAttachmentObjects lists all objects under the given prefix, and generates a pre-signed URL for each one
func (s *S3Repository) listAttachmentObjects(ctx context.Context, bucket, prefix string) ([]dataset.Attachment, error) {
	objs := []dataset.Attachment{}

	input := s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}

	truncated := true
	for truncated {
		output, err := s.S3.ListObjectsV2WithContext(ctx, &input)
		if err != nil {
			return nil, ErrCode("failed to list objects from s3 ", err)
		}

		for _, object := range output.Contents {
			id := strings.TrimPrefix(aws.StringValue(object.Key), prefix)
			if id != "" {
				// generate pre-signed URL for accessing the attachment
				urlStr, err := s.presignURL(bucket, aws.StringValue(object.Key))
				if err != nil {
					log.Errorf("failed to presign request for %s: %s", aws.StringValue(object.Key), err)
				}

				objs = append(objs, dataset.Attachment{
					Name:     strings.TrimPrefix(id, "/"),
					Modified: aws.TimeValue(object.LastModified),
					Size:     aws.Int64Value(object.Size),
					URL:      urlStr,
				})
			}
		}

		truncated = aws.BoolValue(output.IsTruncated)
		input.ContinuationToken = output.NextContinuationToken
	}

	log.Debug(objs)

	return objs, nil
}

func (s *S3Repository) presignURL(bucket, key string) (string, error) {
	req, _ := s.S3.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	return req.Presign(5 * time.Minute)
}
