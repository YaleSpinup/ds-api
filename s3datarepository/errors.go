package s3datarepository

import (
	"github.com/YaleSpinup/ds-api/apierror"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

func ErrCode(msg string, err error) error {
	if aerr, ok := errors.Cause(err).(awserr.Error); ok {
		switch aerr.Code() {
		case
			// Access denied.
			"AccessDenied",

			// There is a problem with your AWS account that prevents the operation from completing successfully.
			"AccountProblem",

			// All access to this Amazon S3 resource has been disabled.
			"AllAccessDisabled",

			// Access forbidden.
			"Forbidden",

			// The AWS access key ID you provided does not exist in our records.
			"InvalidAccessKeyId":

			return apierror.New(apierror.ErrForbidden, msg, aerr)
		case
			// ErrCodeConcurrentModificationException for service response error code
			// "ConcurrentModification".
			//
			// The request was rejected because multiple requests to change this object
			// were submitted simultaneously. Wait a few minutes and submit your request
			// again.
			iam.ErrCodeConcurrentModificationException,

			// ErrCodeDeleteConflictException for service response error code
			// "DeleteConflict".
			//
			// The request was rejected because it attempted to delete a resource that has
			// attached subordinate entities. The error message describes these entities.
			iam.ErrCodeDeleteConflictException,

			// ErrCodeDuplicateCertificateException for service response error code
			// "DuplicateCertificate".
			//
			// The request was rejected because the same certificate is associated with
			// an IAM user in the account.
			iam.ErrCodeDuplicateCertificateException,

			// ErrCodeDuplicateSSHPublicKeyException for service response error code
			// "DuplicateSSHPublicKey".
			//
			// The request was rejected because the SSH public key is already associated
			// with the specified IAM user.
			iam.ErrCodeDuplicateSSHPublicKeyException,

			// ErrCodeEntityAlreadyExistsException for service response error code
			// "EntityAlreadyExists".
			//
			// The request was rejected because it attempted to create a resource that already
			// exists.
			iam.ErrCodeEntityAlreadyExistsException,

			// ErrCodeBucketAlreadyExists for service response error code
			// "BucketAlreadyExists".
			//
			// The requested bucket name is not available. The bucket namespace is shared
			// by all users of the system. Please select a different name and try again.
			s3.ErrCodeBucketAlreadyExists,

			// ErrCodeBucketAlreadyOwnedByYou for service response error code
			// "BucketAlreadyOwnedByYou".
			s3.ErrCodeBucketAlreadyOwnedByYou,

			// 	The bucket you tried to delete is not empty.
			"BucketNotEmpty",

			// The request is not valid with the current state of the bucket.
			"InvalidBucketState",

			// A conflicting conditional operation is currently in progress against this resource. Try again.
			"OperationAborted",

			// Object restore is already in progress.
			"RestoreAlreadyInProgress":
			return apierror.New(apierror.ErrConflict, msg, aerr)
		case
			// ErrCodeNoSuchEntityException for service response error code
			// "NoSuchEntity".
			//
			// The request was rejected because it referenced a resource entity that does
			// not exist. The error message describes the resource.
			iam.ErrCodeNoSuchEntityException,

			// ErrCodeNoSuchBucket for service response error code
			// "NoSuchBucket".
			//
			// The specified bucket does not exist.
			s3.ErrCodeNoSuchBucket,

			// ErrCodeNoSuchKey for service response error code
			// "NoSuchKey".
			//
			// The specified key does not exist.
			s3.ErrCodeNoSuchKey,

			// ErrCodeNoSuchUpload for service response error code
			// "NoSuchUpload".
			//
			// The specified multipart upload does not exist.
			s3.ErrCodeNoSuchUpload,

			// The specified bucket does not exist.
			"NotFound",

			// The specified bucket does not have a bucket policy.
			"NoSuchBucketPolicy",

			// The lifecycle configuration does not exist.
			"NoSuchLifecycleConfiguration",

			// Indicates that the version ID specified in the request does not match an existing version.
			"NoSuchVersion":
			return apierror.New(apierror.ErrNotFound, msg, aerr)

		case
			// ErrCodeCredentialReportExpiredException for service response error code
			// "ReportExpired".
			//
			// The request was rejected because the most recent credential report has expired.
			// To generate a new credential report, use GenerateCredentialReport. For more
			// information about credential report expiration, see Getting Credential Reports
			// (https://docs.aws.amazon.com/IAM/latest/UserGuide/credential-reports.html)
			// in the IAM User Guide.
			iam.ErrCodeCredentialReportExpiredException,

			// ErrCodeCredentialReportNotPresentException for service response error code
			// "ReportNotPresent".
			//
			// The request was rejected because the credential report does not exist. To
			// generate a credential report, use GenerateCredentialReport.
			iam.ErrCodeCredentialReportNotPresentException,

			// ErrCodeCredentialReportNotReadyException for service response error code
			// "ReportInProgress".
			//
			// The request was rejected because the credential report is still being generated.
			iam.ErrCodeCredentialReportNotReadyException,

			// ErrCodeEntityTemporarilyUnmodifiableException for service response error code
			// "EntityTemporarilyUnmodifiable".
			//
			// The request was rejected because it referenced an entity that is temporarily
			// unmodifiable, such as a user name that was deleted and then recreated. The
			// error indicates that the request is likely to succeed if you try again after
			// waiting several minutes. The error message describes the entity.
			iam.ErrCodeEntityTemporarilyUnmodifiableException,

			// ErrCodeInvalidAuthenticationCodeException for service response error code
			// "InvalidAuthenticationCode".
			//
			// The request was rejected because the authentication code was not recognized.
			// The error message describes the specific error.
			iam.ErrCodeInvalidAuthenticationCodeException,

			// ErrCodeInvalidCertificateException for service response error code
			// "InvalidCertificate".
			//
			// The request was rejected because the certificate is invalid.
			iam.ErrCodeInvalidCertificateException,

			// ErrCodeInvalidInputException for service response error code
			// "InvalidInput".
			//
			// The request was rejected because an invalid or out-of-range value was supplied
			// for an input parameter.
			iam.ErrCodeInvalidInputException,

			// ErrCodeInvalidPublicKeyException for service response error code
			// "InvalidPublicKey".
			//
			// The request was rejected because the public key is malformed or otherwise
			// invalid.
			iam.ErrCodeInvalidPublicKeyException,

			// ErrCodeInvalidUserTypeException for service response error code
			// "InvalidUserType".
			//
			// The request was rejected because the type of user for the transaction was
			// incorrect.
			iam.ErrCodeInvalidUserTypeException,

			// ErrCodeKeyPairMismatchException for service response error code
			// "KeyPairMismatch".
			//
			// The request was rejected because the public key certificate and the private
			// key do not match.
			iam.ErrCodeKeyPairMismatchException,

			// ErrCodeMalformedCertificateException for service response error code
			// "MalformedCertificate".
			//
			// The request was rejected because the certificate was malformed or expired.
			// The error message describes the specific error.
			iam.ErrCodeMalformedCertificateException,

			// ErrCodeMalformedPolicyDocumentException for service response error code
			// "MalformedPolicyDocument".
			//
			// The request was rejected because the policy document was malformed. The error
			// message describes the specific error.
			iam.ErrCodeMalformedPolicyDocumentException,

			// ErrCodePasswordPolicyViolationException for service response error code
			// "PasswordPolicyViolation".
			//
			// The request was rejected because the provided password did not meet the requirements
			// imposed by the account password policy.
			iam.ErrCodePasswordPolicyViolationException,

			// ErrCodePolicyEvaluationException for service response error code
			// "PolicyEvaluation".
			//
			// The request failed because a provided policy could not be successfully evaluated.
			// An additional detailed message indicates the source of the failure.
			iam.ErrCodePolicyEvaluationException,

			// ErrCodePolicyNotAttachableException for service response error code
			// "PolicyNotAttachable".
			//
			// The request failed because AWS service role policies can only be attached
			// to the service-linked role for that service.
			iam.ErrCodePolicyNotAttachableException,

			// ErrCodeUnrecognizedPublicKeyEncodingException for service response error code
			// "UnrecognizedPublicKeyEncoding".
			//
			// The request was rejected because the public key encoding format is unsupported
			// or unrecognized.
			iam.ErrCodeUnrecognizedPublicKeyEncodingException,

			// ErrCodeObjectAlreadyInActiveTierError for service response error code
			// "ObjectAlreadyInActiveTierError".
			//
			// This operation is not allowed against this storage tier
			s3.ErrCodeObjectAlreadyInActiveTierError,

			// ErrCodeObjectNotInActiveTierError for service response error code
			// "ObjectNotInActiveTierError".
			//
			// The source object of the COPY operation is not in the active tier and is
			// only stored in Amazon Glacier.
			s3.ErrCodeObjectNotInActiveTierError,

			// The email address you provided is associated with more than one account.
			"AmbiguousGrantByEmailAddress",

			// The authorization header you provided is invalid.
			"AuthorizationHeaderMalformed",

			// The Content-MD5 you specified did not match what we received.
			"BadDigest",

			// This request does not support credentials.
			"CredentialsNotSupported",

			// Cross-location logging not allowed. Buckets in one geographic location cannot log information to a bucket in another location.
			"CrossLocationLoggingProhibited",

			// Your proposed upload is smaller than the minimum allowed object size.
			"EntityTooSmall",

			// Your proposed upload exceeds the maximum allowed object size.
			"EntityTooLarge",

			// The provided token has expired.
			"ExpiredToken",

			// Indicates that the versioning configuration specified in the request is invalid.
			"IllegalVersioningConfigurationException",

			// You did not provide the number of bytes specified by the Content-Length HTTP header
			"IncompleteBody",

			// POST requires exactly one file upload per request.
			"IncorrectNumberOfFilesInPostRequest",

			// Inline data exceeds the maximum allowed size.
			"InlineDataTooLarge",

			// You must specify the Anonymous role.
			"InvalidAddressingHeader",

			// Invalid Argument
			"InvalidArgument",

			// The specified bucket is not valid.
			"InvalidBucketName",

			// 	The Content-MD5 you specified is not valid.
			"InvalidDigest",

			// The encryption request you specified is not valid. The valid value is AES256.
			"InvalidEncryptionAlgorithmError",

			// The operation is not valid for the current state of the object.
			"InvalidObjectState",

			// The specified location constraint is not valid. For more information about Regions.
			"InvalidLocationConstraint",

			// One or more of the specified parts could not be found. The part might not have been uploaded, or the
			// specified entity tag might not have matched the part's entity tag.
			"InvalidPart",

			// The list of parts was not in ascending order. Parts list must be specified in order by part number.
			"InvalidPartOrder",

			// The content of the form does not meet the conditions specified in the policy document.
			"InvalidPolicyDocument",

			// The requested range cannot be satisfied.
			"InvalidRange",

			// Please use AWS4-HMAC-SHA256.
			// SOAP requests must be made over an HTTPS connection.
			// Amazon S3 Transfer Acceleration is not supported for buckets with non-DNS compliant names.
			// Amazon S3 Transfer Acceleration is not supported for buckets with periods (.) in their names.
			// Amazon S3 Transfer Accelerate endpoint only supports virtual style requests.
			// Amazon S3 Transfer Accelerate is not configured on this bucket.
			// Amazon S3 Transfer Accelerate is disabled on this bucket.
			// Amazon S3 Transfer Acceleration is not supported on this bucket. Contact AWS Support for more information.
			// Amazon S3 Transfer Acceleration cannot be enabled on this bucket. Contact AWS Support for more information.
			"InvalidRequest",

			// The SOAP request body is invalid.
			"InvalidSOAPRequest",

			// The storage class you specified is not valid.
			"InvalidStorageClass",

			// The target bucket for logging does not exist, is not owned by you, or does not have the appropriate grants for the log-delivery group.
			"InvalidTargetBucketForLogging",

			//The provided token is malformed or otherwise invalid.
			"InvalidToken",

			// Couldn't parse the specified URI.
			"InvalidURI",

			// Your key is too long.
			"KeyTooLongError",

			// The XML you provided was not well-formed or did not validate against our published schema.
			"MalformedACLError",

			// The body of your POST request is not well-formed multipart/form-data.
			"MalformedPOSTRequest",

			// This happens when the user sends malformed XML (XML that doesn't conform to the published XSD) for the configuration.
			// The error message is, "The XML you provided was not well-formed or did not validate against our published schema."
			"MalformedXML",

			// The specified method is not allowed against this resource.
			"MethodNotAllowed",

			// A SOAP attachment was expected, but none were found.
			"MissingAttachment",

			// You must provide the Content-Length HTTP header.
			"MissingContentLength",

			// This happens when the user sends an empty XML document as a request. The error message is, "Request body is empty."
			"MissingRequestBodyError",

			// The SOAP 1.1 request is missing a security element.
			"MissingSecurityElement",

			// Your request is missing a required header.
			"MissingSecurityHeader",

			// There is no such thing as a logging status subresource for a key.
			"NoLoggingStatusForKey",

			// At least one of the preconditions you specified did not hold.
			"PreconditionFailed",

			// Bucket POST must be of the enclosure-type multipart/form-data.
			"RequestIsNotMultiPartContent",

			// Requesting the torrent file of a bucket is not permitted.
			"RequestTorrentOfBucketError",

			// The request signature we calculated does not match the signature you provided.
			// Check your AWS secret access key and signing method.
			"SignatureDoesNotMatch",

			// The provided token must be refreshed.
			"TokenRefreshRequired",

			// This request does not support content.
			"UnexpectedContent",

			// The email address you provided does not match any account on record.
			"UnresolvableGrantByEmailAddress",

			// The bucket POST must contain the specified field name. If it is specified, check the order of the fields.
			"UserKeyMustBeSpecified":

			return apierror.New(apierror.ErrBadRequest, msg, aerr)
		case
			// ErrCodeLimitExceededException for service response error code
			// "LimitExceeded".
			//
			// The request was rejected because it attempted to create resources beyond
			// the current AWS account limits. The error message describes the limit exceeded.
			iam.ErrCodeLimitExceededException,

			// ErrCodeReportGenerationLimitExceededException for service response error code
			// "ReportGenerationLimitExceeded".
			//
			// The request failed because the maximum number of concurrent requests for
			// this account are already running.
			iam.ErrCodeReportGenerationLimitExceededException,

			// Your request was too big.
			"MaxMessageLengthExceeded",

			// Your POST request fields preceding the upload file were too large.
			"MaxPostPreDataLengthExceededError",

			// Your metadata headers exceed the maximum allowed metadata size.
			"MetadataTooLarge",

			// Reduce your request rate.
			"ServiceUnavailable",

			// Reduce your request rate.
			"SlowDown",

			// You have attempted to create more buckets than allowed.
			"TooManyBuckets":

			return apierror.New(apierror.ErrLimitExceeded, msg, aerr)
		case
			// ErrCodeServiceFailureException for service response error code
			// "ServiceFailure".
			//
			// The request processing has failed because of an unknown error, exception
			// or failure.
			iam.ErrCodeServiceFailureException,

			// ErrCodeServiceNotSupportedException for service response error code
			// "NotSupportedService".
			//
			// The specified service does not support service-specific credentials.
			iam.ErrCodeServiceNotSupportedException,

			// All access to this object has been disabled. Please contact AWS Support for further assistance.
			"InvalidPayer",

			// We encountered an internal error. Please try again.
			"InternalError",

			// The provided security credentials are not valid.
			"InvalidSecurity",

			// A header you provided implies functionality that is not implemented.
			"NotImplemented",

			// Your account is not signed up for the Amazon S3 service. You must sign up before you can use Amazon S3.
			// You can sign up at the following URL: https://aws.amazon.com/s3
			"NotSignedUp",

			// The bucket you are attempting to access must be addressed using the specified endpoint. Send all future requests to this endpoint.
			"PermanentRedirect",

			// Temporary redirect.
			"Redirect",

			// Your socket connection to the server was not read from or written to within the timeout period.
			"RequestTimeout",

			// The difference between the request time and the server's time is too large.
			"RequestTimeTooSkewed",

			// You are being redirected to the bucket while DNS updates.
			"TemporaryRedirect":
			return apierror.New(apierror.ErrServiceUnavailable, msg, aerr)
		default:
			m := msg + ": " + aerr.Message()
			return apierror.New(apierror.ErrBadRequest, m, aerr)
		}
	}

	log.Warnf("uncaught error: %s, returning Internal Server Error", err)
	return apierror.New(apierror.ErrInternalError, msg, err)
}
