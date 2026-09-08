"""Convert Amazon Transcribe JSON output to plain text in S3."""

import json
import logging
import os
from urllib.parse import unquote_plus

import boto3
from botocore.exceptions import ClientError


logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())

s3_client = boto3.client("s3")
DESTINATION_BUCKET = "dtsc-671-transcribe-output-txt-fa1-2026-cnielson"


def lambda_handler(event, context):
	"""Process each S3 ObjectCreated record in the Lambda event."""
	request_id = getattr(context, "aws_request_id", "unknown")
	records = event.get("Records", [])

	logger.info(
		"Lambda invocation started",
		extra={"request_id": request_id, "record_count": len(records)},
	)

	if not records:
		logger.error("The event does not contain any S3 records")
		raise ValueError("The event does not contain any S3 records")

	results = [process_s3_record(record) for record in records]

	logger.info(
		"Lambda invocation completed successfully",
		extra={"request_id": request_id, "processed_count": len(results)},
	)

	return {
		"statusCode": 200,
		"body": json.dumps({"results": results}),
	}


def process_s3_record(record):
	"""Read one Transcribe JSON object and write its transcript as text."""
	try:
		s3_record = record["s3"]
		source_bucket = s3_record["bucket"]["name"]
		source_key = unquote_plus(s3_record["object"]["key"])

		logger.info(
			"Processing Transcribe JSON object",
			extra={
				"source_bucket": source_bucket,
				"source_key": source_key,
				"event_name": record.get("eventName"),
			},
		)

		if not source_key.lower().endswith(".json"):
			raise ValueError(f"Unsupported file type; expected .json: {source_key}")

		source_json = read_json_object(source_bucket, source_key)
		transcript_text = extract_transcript_text(source_json)
		output_key = replace_extension(source_key, ".txt")

		write_text_object(DESTINATION_BUCKET, output_key, transcript_text)

		output_uri = f"s3://{DESTINATION_BUCKET}/{output_key}"
		logger.info(
			"Transcript text written successfully",
			extra={
				"source_bucket": source_bucket,
				"source_key": source_key,
				"destination_bucket": DESTINATION_BUCKET,
				"output_key": output_key,
				"character_count": len(transcript_text),
			},
		)

		return {
			"source": f"s3://{source_bucket}/{source_key}",
			"output": output_uri,
			"character_count": len(transcript_text),
		}

	except KeyError as error:
		logger.exception("Malformed S3 event record")
		raise ValueError(f"Missing required event field: {error}") from error
	except ClientError:
		logger.exception("AWS API error while converting Transcribe JSON")
		raise
	except json.JSONDecodeError:
		logger.exception("The S3 object is not valid JSON")
		raise ValueError(f"Invalid JSON in s3://{source_bucket}/{source_key}")
	except UnicodeDecodeError:
		logger.exception("The S3 object is not valid UTF-8")
		raise ValueError(f"The source file must be UTF-8 encoded: {source_key}")
	except Exception:
		logger.exception("Unexpected error while processing S3 record")
		raise


def read_json_object(bucket_name, object_key):
	"""Download and parse a UTF-8 JSON object from S3."""
	logger.info(
		"Reading Transcribe JSON from S3",
		extra={"source_bucket": bucket_name, "source_key": object_key},
	)

	response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
	body = response["Body"].read()
	logger.info(
		"Transcribe JSON downloaded",
		extra={"source_key": object_key, "size_bytes": len(body)},
	)

	return json.loads(body.decode("utf-8"))


def extract_transcript_text(transcribe_json):
	"""Extract the combined transcript text from standard Transcribe output."""
	try:
		transcripts = transcribe_json["results"]["transcripts"]
		if not transcripts or not isinstance(transcripts[0], dict):
			raise ValueError("Transcribe JSON contains no transcript entry")

		transcript_text = transcripts[0].get("transcript", "")
		if not isinstance(transcript_text, str) or not transcript_text.strip():
			raise ValueError("Transcribe JSON contains an empty transcript")

		return transcript_text.strip() + "\n"
	except (KeyError, TypeError) as error:
		raise ValueError(
			"JSON does not contain the expected Transcribe structure: "
			"results.transcripts[0].transcript"
		) from error


def write_text_object(bucket_name, object_key, text):
	"""Upload transcript text to the destination S3 bucket."""
	logger.info(
		"Writing transcript text to S3",
		extra={"destination_bucket": bucket_name, "output_key": object_key},
	)

	s3_client.put_object(
		Bucket=bucket_name,
		Key=object_key,
		Body=text.encode("utf-8"),
		ContentType="text/plain; charset=utf-8",
	)


def replace_extension(object_key, new_extension):
	"""Replace the source extension while preserving its object path."""
	directory, filename = object_key.rsplit("/", 1) if "/" in object_key else ("", object_key)
	base_name = filename.rsplit(".", 1)[0]
	output_filename = f"{base_name}{new_extension}"
	return f"{directory}/{output_filename}" if directory else output_filename
