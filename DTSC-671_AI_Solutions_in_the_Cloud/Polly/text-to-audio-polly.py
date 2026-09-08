import json
import logging
import os
from urllib.parse import unquote_plus

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())

s3_client = boto3.client("s3")
polly_client = boto3.client("polly")

DESTINATION_BUCKET = "dtsc-671-polly-output-fa1-2026-cnielson"

def lambda_handler(event, context):
    """Convert S3 .txt objects to MP3 files using Amazon Polly."""
    logger.info(
        "Lambda invocation started",
        extra={
            "request_id": getattr(context, "aws_request_id", "unknown"),
            "record_count": len(event.get("Records", [])),
        },
    )

    results = []

    try:
        records = event.get("Records", [])
        if not records:
            raise ValueError("The event does not contain any S3 records")

        for record in records:
            results.append(process_s3_record(record))

        logger.info("Lambda invocation completed successfully")
        return {
            "statusCode": 200,
            "body": json.dumps({"results": results}),
        }

    except Exception:
        logger.exception("Lambda invocation failed")
        raise


def process_s3_record(record):
    """Process one S3 ObjectCreated event record."""
    try:
        s3_data = record["s3"]
        bucket_name = s3_data["bucket"]["name"]
        source_key = unquote_plus(s3_data["object"]["key"])

        logger.info(
            "Processing S3 object",
            extra={
                "bucket": bucket_name,
                "source_key": source_key,
                "event_name": record.get("eventName"),
            },
        )

        if not source_key.lower().endswith(".txt"):
            raise ValueError(f"Unsupported file type: {source_key}")

        source_text = read_text_file(bucket_name, source_key)

        if not source_text.strip():
            raise ValueError(f"S3 object is empty: s3://{bucket_name}/{source_key}")

        if len(source_text) > 3000:
            raise ValueError(
                "Amazon Polly standard synthesis supports a maximum of "
                "3,000 billed characters per request"
            )

        audio_bytes = synthesize_speech(source_text)
        output_key = replace_extension(source_key, ".mp3")

        write_mp3_file(DESTINATION_BUCKET, output_key, audio_bytes)

        logger.info(
            "Text-to-speech conversion completed",
            extra={
                "source_key": source_key,
                "output_key": output_key,
                "audio_size_bytes": len(audio_bytes),
            },
        )

        return {
            "source": f"s3://{bucket_name}/{source_key}",
            "output": f"s3://{bucket_name}/{output_key}",
            "audio_size_bytes": len(audio_bytes),
        }

    except KeyError as error:
        logger.exception("Malformed S3 event record")
        raise ValueError(f"Missing required event field: {error}") from error

    except ClientError:
        logger.exception("AWS API error while processing S3 record")
        raise

    except UnicodeDecodeError:
        logger.exception("The S3 object is not valid UTF-8 text")
        raise ValueError("The source file must be a UTF-8 encoded text file")

    except Exception:
        logger.exception("Unexpected error while processing S3 record")
        raise


def read_text_file(bucket_name, object_key):
    """Read a UTF-8 text file from S3."""
    logger.info(
        "Reading source text from S3",
        extra={"bucket": bucket_name, "source_key": object_key},
    )

    response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    body = response["Body"].read()

    logger.info(
        "Source text read successfully",
        extra={"source_key": object_key, "size_bytes": len(body)},
    )

    return body.decode("utf-8")


def synthesize_speech(text):
    """Convert text to MP3 audio using Amazon Polly."""
    voice_id = os.getenv("POLLY_VOICE_ID", "Joanna")
    language_code = os.getenv("POLLY_LANGUAGE_CODE", "en-US")

    logger.info(
        "Calling Amazon Polly",
        extra={
            "voice_id": voice_id,
            "language_code": language_code,
            "character_count": len(text),
        },
    )

    response = polly_client.synthesize_speech(
        Text=text,
        TextType="text",
        OutputFormat="mp3",
        VoiceId=voice_id,
        LanguageCode=language_code,
    )

    audio_stream = response.get("AudioStream")
    if audio_stream is None:
        raise RuntimeError("Amazon Polly returned no AudioStream")

    audio_bytes = audio_stream.read()
    if not audio_bytes:
        raise RuntimeError("Amazon Polly returned an empty audio stream")

    logger.info(
        "Amazon Polly synthesis completed",
        extra={"audio_size_bytes": len(audio_bytes)},
    )

    return audio_bytes


def write_mp3_file(bucket_name, object_key, audio_bytes):
    """Write the generated MP3 file to the source S3 bucket."""
    logger.info(
        "Writing generated MP3 to S3",
        extra={"bucket": bucket_name, "output_key": object_key},
    )

    s3_client.put_object(
        Bucket=bucket_name,
        Key=object_key,
        Body=audio_bytes,
        ContentType="audio/mpeg",
    )

    logger.info(
        "Generated MP3 written successfully",
        extra={"bucket": bucket_name, "output_key": object_key},
    )


def replace_extension(object_key, new_extension):
    """Replace the source file extension while preserving its path."""
    filename = object_key.rsplit("/", 1)[-1]
    directory = object_key[: -len(filename)] if "/" in object_key else ""

    base_name = filename.rsplit(".", 1)[0]
    return f"{directory}{base_name}{new_extension}"

