import json
import logging
import os
from urllib.parse import unquote_plus

import boto3
from botocore.exceptions import ClientError, ParamValidationError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3", region_name=os.environ.get("AWS_REGION"))
rekognition_client = boto3.client("rekognition", region_name=os.environ.get("AWS_REGION"))

OUTPUT_BUCKET='dtsc-671-rekognition-output-fa1-2026-cnielson'
MAX_LABELS=50
MIN_CONFIDENCE=50

def _build_output_key(source_key: str) -> str:
    """
    Convert:
      folder/image.jpg -> folder/image.json
      image.jpg -> image.json
    """
    source_key = unquote_plus(source_key)

    if not source_key:
        raise ValueError("Source key is empty.")

    if "/" in source_key:
        prefix, filename = source_key.rsplit("/", 1)
        base_name = filename.rsplit(".", 1)[0]
        return f"{prefix}/{base_name}.json"
    else:
        base_name = source_key.rsplit(".", 1)[0]
        return f"{base_name}.json"


def _is_supported_image(key: str) -> bool:
    lower_key = key.lower()
    return lower_key.endswith(".jpg") or lower_key.endswith(".jpeg")


def _safe_json_dump(data: dict) -> str:
    return json.dumps(data, default=str, indent=2, ensure_ascii=False)


def lambda_handler(event, context):
    logger.info("Received event: %s", json.dumps(event, default=str))

    try:
        if not event or "Records" not in event or not event["Records"]:
            raise ValueError("Event does not contain any S3 Records.")

        record = event["Records"][0]
        bucket_name = record["s3"]["bucket"]["name"]
        source_key = unquote_plus(record["s3"]["object"]["key"])
        source_size = record["s3"]["object"].get("size")

        logger.info(
            "Processing S3 object: bucket=%s, key=%s, size=%s bytes",
            bucket_name,
            source_key,
            source_size,
        )

        if not _is_supported_image(source_key):
            raise ValueError(f"Unsupported object type: {source_key}. Expected .jpg or .jpeg")

        if source_size is not None and int(source_size) == 0:
            raise ValueError(f"Source object is empty: {source_key}")

        target_bucket = OUTPUT_BUCKET
        output_key = _build_output_key(source_key)

        logger.info(
            "Target bucket configured: bucket=%s, output_key=%s",
            target_bucket,
            output_key,
        )

        # Read image bytes from S3
        logger.info("Downloading image from S3...")
        response = s3_client.get_object(Bucket=bucket_name, Key=source_key)
        file_bytes = response["Body"].read()

        if not file_bytes:
            raise ValueError(f"Downloaded image is empty for key: {source_key}")

        logger.info(
            "Image downloaded successfully. bytes=%s, content_type=%s",
            len(file_bytes),
            response.get("ContentType"),
        )

        # Call Rekognition
        logger.info("Calling Rekognition detect_labels...")
        detect_response = rekognition_client.detect_labels(
            Image={"Bytes": file_bytes},
            MaxLabels=MAX_LABELS,
            MinConfidence=MIN_CONFIDENCE,
        )

        labels = detect_response.get("Labels", [])
        logger.info("Rekognition returned %s labels", len(labels))

        # Build JSON payload
        payload = {
            "sourceBucket": bucket_name,
            "sourceKey": source_key,
            "targetBucket": target_bucket,
            "targetKey": output_key,
            "imageType": "jpg",
            "labels": [
                {
                    "Name": label.get("Name"),
                    "Confidence": float(label.get("Confidence", 0.0)),
                    "Instances": label.get("Instances", []),
                    "Parents": label.get("Parents", []),
                    "Aliases": label.get("Aliases", []),
                }
                for label in labels
            ],
            "labelCount": len(labels),
            "status": "success",
        }

        json_body = _safe_json_dump(payload)

        # Write JSON to target bucket
        logger.info("Writing JSON output to S3...")
        s3_client.put_object(
            Bucket=target_bucket,
            Key=output_key,
            Body=json_body.encode("utf-8"),
            ContentType="application/json",
            ServerSideEncryption="AES256",
        )

        logger.info(
            "Successfully wrote output JSON: bucket=%s, key=%s",
            target_bucket,
            output_key,
        )

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "Image analyzed successfully",
                    "sourceBucket": bucket_name,
                    "sourceKey": source_key,
                    "targetBucket": target_bucket,
                    "targetKey": output_key,
                    "labelCount": len(labels),
                }
            ),
        }

    except ParamValidationError as e:
        logger.exception("Parameter validation error while processing image: %s", e)
        raise

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        error_message = e.response.get("Error", {}).get("Message", str(e))
        logger.exception(
            "AWS SDK error while processing S3/Rekognition request: code=%s, message=%s",
            error_code,
            error_message,
        )
        raise

    except ValueError as e:
        logger.exception("Validation error: %s", str(e))
        raise

    except Exception as e:
        logger.exception("Unhandled exception in lambda_handler: %s", str(e))
        raise