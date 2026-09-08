"""Start an Amazon Transcribe job for each S3 .mp4 ObjectCreated event."""

import json
import logging
import os
import re
import uuid
from urllib.parse import unquote_plus

import boto3
from botocore.exceptions import ClientError


logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())

s3_client = boto3.client("s3")
transcribe_client = boto3.client("transcribe")

DESTINATION_BUCKET = "dtsc-671-transcribe-output-json-fa1-2026-cnielson"
DEFAULT_LANGUAGE_CODE = "en-US"


def lambda_handler(event, context):
	"""Start transcription jobs for S3 ObjectCreated records."""
	request_id = getattr(context, "aws_request_id", "unknown")
	records = event.get("Records", [])

	logger.info(
		"Lambda invocation started",
		extra={"request_id": request_id, "record_count": len(records)},
	)

	if not records:
		logger.error("The event does not contain any S3 records")
		raise ValueError("The event does not contain any S3 records")

	results = []
	for record in records:
		results.append(process_s3_record(record))

	logger.info(
		"Lambda invocation completed successfully",
		extra={"request_id": request_id, "processed_count": len(results)},
	)

	return {
		"statusCode": 200,
		"body": json.dumps({"results": results}),
	}


def process_s3_record(record):
	"""Validate one S3 event record and start its transcription job."""
	try:
		s3_record = record["s3"]
		source_bucket = s3_record["bucket"]["name"]
		source_key = unquote_plus(s3_record["object"]["key"])
		source_version = s3_record["object"].get("versionId")
		source_etag = s3_record["object"].get("eTag", "")

		logger.info(
			"Processing S3 object",
			extra={
				"source_bucket": source_bucket,
				"source_key": source_key,
				"event_name": record.get("eventName"),
			},
		)

		if not source_key.lower().endswith(".mp4"):
			raise ValueError(f"Unsupported file type; expected .mp4: {source_key}")

		source_metadata = verify_source_object(source_bucket, source_key)
		output_key = replace_extension(source_key, ".json")
		job_name = build_job_name(source_bucket, source_key, source_version, source_etag)
		media_uri = f"s3://{source_bucket}/{source_key}"

		start_args = {
			"TranscriptionJobName": job_name,
			"Media": {"MediaFileUri": media_uri},
			"MediaFormat": "mp4",
			"LanguageCode": os.getenv("TRANSCRIBE_LANGUAGE_CODE", DEFAULT_LANGUAGE_CODE),
			"OutputBucketName": DESTINATION_BUCKET,
			"OutputKey": output_key,
		}

		logger.info(
			"Starting Amazon Transcribe job",
			extra={
				"job_name": job_name,
				"media_uri": media_uri,
				"destination_bucket": DESTINATION_BUCKET,
				"output_key": output_key,
				"source_size_bytes": source_metadata.get("ContentLength"),
			},
		)

		response = transcribe_client.start_transcription_job(**start_args)
		transcription_job = response.get("TranscriptionJob", {})
		job_status = transcription_job.get("TranscriptionJobStatus", "UNKNOWN")

		logger.info(
			"Amazon Transcribe job started",
			extra={"job_name": job_name, "job_status": job_status},
		)

		return {
			"source": media_uri,
			"job_name": job_name,
			"job_status": job_status,
			"output": f"s3://{DESTINATION_BUCKET}/{output_key}",
		}

	except KeyError as error:
		logger.exception("Malformed S3 event record")
		raise ValueError(f"Missing required event field: {error}") from error
	except ClientError:
		logger.exception("AWS API error while starting transcription job")
		raise
	except Exception:
		logger.exception("Unexpected error while processing S3 record")
		raise


def verify_source_object(bucket_name, object_key):
	"""Confirm that the source object exists and is readable by this Lambda."""
	logger.info(
		"Checking source S3 object",
		extra={"source_bucket": bucket_name, "source_key": object_key},
	)
	response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
	logger.info(
		"Source S3 object verified",
		extra={
			"source_bucket": bucket_name,
			"source_key": object_key,
			"content_length": response.get("ContentLength"),
			"content_type": response.get("ContentType"),
		},
	)
	return response


def replace_extension(object_key, new_extension):
	"""Replace the source file extension while preserving its object path."""
	directory, filename = object_key.rsplit("/", 1) if "/" in object_key else ("", object_key)
	base_name = filename.rsplit(".", 1)[0]
	output_filename = f"{base_name}{new_extension}"
	return f"{directory}/{output_filename}" if directory else output_filename


def build_job_name(bucket_name, object_key, version_id=None, etag=""):
	"""Build a unique, Transcribe-compatible job name from the source object."""
	identity = f"{bucket_name}-{object_key}-{version_id or etag}"
	sanitized = re.sub(r"[^0-9A-Za-z-]", "-", identity).strip("-")
	unique_id = uuid.uuid4().hex
	return f"s3-{sanitized[:155]}-{unique_id}"
