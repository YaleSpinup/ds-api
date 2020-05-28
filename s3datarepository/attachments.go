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
	log.Infof("creating attachment for data set '%s': %s", id, attachmentName)

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

	log.Infof("uploading attachment '%s' to s3datarepository: %s", attachmentName, name)

	if _, err := s.S3Uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket: aws.String(name),
		Key:    aws.String(attachmentName),
		Body:   attachmentBody,
	}); err != nil {
		return ErrCode("failed to upload attachment to s3 bucket "+name, err)
	}

	return nil
}

// DeleteAttachment deletes an attachment from the data repository
func (s *S3Repository) DeleteAttachment(ctx context.Context, id, attachmentName string) error {
	log.Infof("deleting attachment from data set '%s': %s", id, attachmentName)

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

	return s.deleteObject(ctx, name, attachmentName)
}

func (s *S3Repository) deleteObject(ctx context.Context, bucket, key string) error {
	log.Infof("deleting object with key '%s' from bucket %s", key, bucket)

	if _, err := s.S3.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}); err != nil {
		return ErrCode("failed to delete object "+key, err)
	}

	return nil
}

// ListAttachments lists all attachments for the data repository
func (s *S3Repository) ListAttachments(ctx context.Context, id string, showURL bool) ([]dataset.Attachment, error) {
	if id == "" {
		return nil, apierror.New(apierror.ErrBadRequest, "invalid input", errors.New("empty id"))
	}

	name := id
	if s.NamePrefix != "" {
		name = s.NamePrefix + "-" + name
	}

	log.Debugf("getting list of attachments for s3datarepository: %s (showURL: %t)", name, showURL)

	return s.listAttachmentObjects(ctx, name, attachmentsPrefix, showURL)
}

// listAttachmentObjects lists all objects under the given prefix, and generates a pre-signed URL for each one
func (s *S3Repository) listAttachmentObjects(ctx context.Context, bucket, prefix string, showURL bool) ([]dataset.Attachment, error) {
	attachments := []dataset.Attachment{}

	input := s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}

	truncated := true
	for truncated {
		output, err := s.S3.ListObjectsV2WithContext(ctx, &input)
		if err != nil {
			return nil, ErrCode("failed to list objects from s3", err)
		}

		for _, object := range output.Contents {
			id := strings.TrimPrefix(aws.StringValue(object.Key), prefix)
			if id != "" {
				attachment := dataset.Attachment{
					Name:     strings.TrimPrefix(id, "/"),
					Modified: aws.TimeValue(object.LastModified),
					Size:     aws.Int64Value(object.Size),
				}

				if showURL {
					// generate pre-signed URL for accessing the attachment
					urlStr, err := s.presignURL(bucket, aws.StringValue(object.Key))
					if err != nil {
						log.Errorf("failed to presign request for %s: %s", aws.StringValue(object.Key), err)
					} else {
						attachment.URL = urlStr
					}
				}

				attachments = append(attachments, attachment)
			}
		}

		truncated = aws.BoolValue(output.IsTruncated)
		input.ContinuationToken = output.NextContinuationToken
	}

	log.Debug(attachments)

	return attachments, nil
}

func (s *S3Repository) presignURL(bucket, key string) (string, error) {
	req, _ := s.S3.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	return req.Presign(5 * time.Minute)
}
