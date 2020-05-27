package s3datarepository

import (
	"testing"

	"github.com/YaleSpinup/ds-api/apierror"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
)

func TestErrCode(t *testing.T) {
	apiErrorTestCases := map[string]string{
		"": apierror.ErrBadRequest,

		"AccessDenied":       apierror.ErrForbidden,
		"AccountProblem":     apierror.ErrForbidden,
		"AllAccessDisabled":  apierror.ErrForbidden,
		"Forbidden":          apierror.ErrForbidden,
		"InvalidAccessKeyId": apierror.ErrForbidden,

		iam.ErrCodeConcurrentModificationException: apierror.ErrConflict,
		iam.ErrCodeDeleteConflictException:         apierror.ErrConflict,
		iam.ErrCodeDuplicateCertificateException:   apierror.ErrConflict,
		iam.ErrCodeDuplicateSSHPublicKeyException:  apierror.ErrConflict,
		iam.ErrCodeEntityAlreadyExistsException:    apierror.ErrConflict,
		s3.ErrCodeBucketAlreadyExists:              apierror.ErrConflict,
		s3.ErrCodeBucketAlreadyOwnedByYou:          apierror.ErrConflict,
		"BucketNotEmpty":                           apierror.ErrConflict,
		"InvalidBucketState":                       apierror.ErrConflict,
		"OperationAborted":                         apierror.ErrConflict,
		"RestoreAlreadyInProgress":                 apierror.ErrConflict,

		iam.ErrCodeNoSuchEntityException: apierror.ErrNotFound,
		s3.ErrCodeNoSuchBucket:           apierror.ErrNotFound,
		s3.ErrCodeNoSuchKey:              apierror.ErrNotFound,
		s3.ErrCodeNoSuchUpload:           apierror.ErrNotFound,
		"NotFound":                       apierror.ErrNotFound,
		"NoSuchBucketPolicy":             apierror.ErrNotFound,
		"NoSuchLifecycleConfiguration":   apierror.ErrNotFound,
		"NoSuchVersion":                  apierror.ErrNotFound,

		iam.ErrCodeCredentialReportExpiredException:       apierror.ErrBadRequest,
		iam.ErrCodeCredentialReportNotPresentException:    apierror.ErrBadRequest,
		iam.ErrCodeCredentialReportNotReadyException:      apierror.ErrBadRequest,
		iam.ErrCodeEntityTemporarilyUnmodifiableException: apierror.ErrBadRequest,
		iam.ErrCodeInvalidAuthenticationCodeException:     apierror.ErrBadRequest,
		iam.ErrCodeInvalidCertificateException:            apierror.ErrBadRequest,
		iam.ErrCodeInvalidInputException:                  apierror.ErrBadRequest,
		iam.ErrCodeInvalidPublicKeyException:              apierror.ErrBadRequest,
		iam.ErrCodeInvalidUserTypeException:               apierror.ErrBadRequest,
		iam.ErrCodeKeyPairMismatchException:               apierror.ErrBadRequest,
		iam.ErrCodeMalformedCertificateException:          apierror.ErrBadRequest,
		iam.ErrCodeMalformedPolicyDocumentException:       apierror.ErrBadRequest,
		iam.ErrCodePasswordPolicyViolationException:       apierror.ErrBadRequest,
		iam.ErrCodePolicyEvaluationException:              apierror.ErrBadRequest,
		iam.ErrCodePolicyNotAttachableException:           apierror.ErrBadRequest,
		iam.ErrCodeUnrecognizedPublicKeyEncodingException: apierror.ErrBadRequest,
		s3.ErrCodeObjectAlreadyInActiveTierError:          apierror.ErrBadRequest,
		s3.ErrCodeObjectNotInActiveTierError:              apierror.ErrBadRequest,
		"AmbiguousGrantByEmailAddress":                    apierror.ErrBadRequest,
		"AuthorizationHeaderMalformed":                    apierror.ErrBadRequest,
		"BadDigest":                                       apierror.ErrBadRequest,
		"CredentialsNotSupported":                         apierror.ErrBadRequest,
		"CrossLocationLoggingProhibited":                  apierror.ErrBadRequest,
		"EntityTooSmall":                                  apierror.ErrBadRequest,
		"EntityTooLarge":                                  apierror.ErrBadRequest,
		"ExpiredToken":                                    apierror.ErrBadRequest,
		"IllegalVersioningConfigurationException":         apierror.ErrBadRequest,
		"IncompleteBody":                                  apierror.ErrBadRequest,
		"IncorrectNumberOfFilesInPostRequest":             apierror.ErrBadRequest,
		"InlineDataTooLarge":                              apierror.ErrBadRequest,
		"InvalidAddressingHeader":                         apierror.ErrBadRequest,
		"InvalidArgument":                                 apierror.ErrBadRequest,
		"InvalidBucketName":                               apierror.ErrBadRequest,
		"InvalidDigest":                                   apierror.ErrBadRequest,
		"InvalidEncryptionAlgorithmError":                 apierror.ErrBadRequest,
		"InvalidObjectState":                              apierror.ErrBadRequest,
		"InvalidLocationConstraint":                       apierror.ErrBadRequest,
		"InvalidPart":                                     apierror.ErrBadRequest,
		"InvalidPartOrder":                                apierror.ErrBadRequest,
		"InvalidPolicyDocument":                           apierror.ErrBadRequest,
		"InvalidRange":                                    apierror.ErrBadRequest,
		"InvalidRequest":                                  apierror.ErrBadRequest,
		"InvalidSOAPRequest":                              apierror.ErrBadRequest,
		"InvalidStorageClass":                             apierror.ErrBadRequest,
		"InvalidTargetBucketForLogging":                   apierror.ErrBadRequest,
		"InvalidToken":                                    apierror.ErrBadRequest,
		"InvalidURI":                                      apierror.ErrBadRequest,
		"KeyTooLongError":                                 apierror.ErrBadRequest,
		"MalformedACLError":                               apierror.ErrBadRequest,
		"MalformedPOSTRequest":                            apierror.ErrBadRequest,
		"MalformedXML":                                    apierror.ErrBadRequest,
		"MethodNotAllowed":                                apierror.ErrBadRequest,
		"MissingAttachment":                               apierror.ErrBadRequest,
		"MissingContentLength":                            apierror.ErrBadRequest,
		"MissingRequestBodyError":                         apierror.ErrBadRequest,
		"MissingSecurityElement":                          apierror.ErrBadRequest,
		"MissingSecurityHeader":                           apierror.ErrBadRequest,
		"NoLoggingStatusForKey":                           apierror.ErrBadRequest,
		"PreconditionFailed":                              apierror.ErrBadRequest,
		"RequestIsNotMultiPartContent":                    apierror.ErrBadRequest,
		"RequestTorrentOfBucketError":                     apierror.ErrBadRequest,
		"SignatureDoesNotMatch":                           apierror.ErrBadRequest,
		"TokenRefreshRequired":                            apierror.ErrBadRequest,
		"UnexpectedContent":                               apierror.ErrBadRequest,
		"UnresolvableGrantByEmailAddress":                 apierror.ErrBadRequest,
		"UserKeyMustBeSpecified":                          apierror.ErrBadRequest,

		iam.ErrCodeLimitExceededException:                 apierror.ErrLimitExceeded,
		iam.ErrCodeReportGenerationLimitExceededException: apierror.ErrLimitExceeded,
		"MaxMessageLengthExceeded":                        apierror.ErrLimitExceeded,
		"MaxPostPreDataLengthExceededError":               apierror.ErrLimitExceeded,
		"MetadataTooLarge":                                apierror.ErrLimitExceeded,
		"ServiceUnavailable":                              apierror.ErrLimitExceeded,
		"SlowDown":                                        apierror.ErrLimitExceeded,
		"TooManyBuckets":                                  apierror.ErrLimitExceeded,

		iam.ErrCodeServiceFailureException:      apierror.ErrServiceUnavailable,
		iam.ErrCodeServiceNotSupportedException: apierror.ErrServiceUnavailable,
		"InvalidPayer":                          apierror.ErrServiceUnavailable,
		"InternalError":                         apierror.ErrServiceUnavailable,
		"InvalidSecurity":                       apierror.ErrServiceUnavailable,
		"NotImplemented":                        apierror.ErrServiceUnavailable,
		"NotSignedUp":                           apierror.ErrServiceUnavailable,
		"PermanentRedirect":                     apierror.ErrServiceUnavailable,
		"Redirect":                              apierror.ErrServiceUnavailable,
		"RequestTimeout":                        apierror.ErrServiceUnavailable,
		"RequestTimeTooSkewed":                  apierror.ErrServiceUnavailable,
		"TemporaryRedirect":                     apierror.ErrServiceUnavailable,
	}

	for awsErr, apiErr := range apiErrorTestCases {
		err := ErrCode("test error", awserr.New(awsErr, awsErr, nil))
		if aerr, ok := errors.Cause(err).(apierror.Error); ok {
			t.Logf("got apierror '%s'", aerr)
		} else {
			t.Errorf("expected cloudwatch error %s to be an apierror.Error %s, got %s", awsErr, apiErr, err)
		}
	}

	err := ErrCode("test error", errors.New("Unknown"))
	if aerr, ok := errors.Cause(err).(apierror.Error); ok {
		t.Logf("got apierror '%s'", aerr)
	} else {
		t.Errorf("expected unknown error to be an apierror.ErrInternalError, got %s", err)
	}
}
