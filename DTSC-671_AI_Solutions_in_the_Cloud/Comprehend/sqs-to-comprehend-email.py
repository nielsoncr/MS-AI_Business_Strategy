"""Detect product-review sentiment from SQS messages and publish results to SNS."""

import json
import logging
import os

import boto3
from botocore.exceptions import ClientError


logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())

comprehend_client = boto3.client("comprehend")
sns_client = boto3.client("sns")

DEFAULT_LANGUAGE_CODE = "en"
MAX_REVIEW_BYTES = 5_000


def lambda_handler(event, context):
	"""Process an SQS batch and return failed message IDs for partial retries."""
	request_id = getattr(context, "aws_request_id", "unknown")
	records = event.get("Records", [])
	topic_arn = os.getenv("SNS_TOPIC_ARN")

	logger.info(
		"Lambda invocation started",
		extra={"request_id": request_id, "record_count": len(records)},
	)

	if not topic_arn:
		logger.error("SNS_TOPIC_ARN environment variable is not configured")
		raise RuntimeError("SNS_TOPIC_ARN environment variable is required")

	if not records:
		logger.warning("SQS event contains no records")
		return {"batchItemFailures": []}

	failed_items = []
	processed_count = 0

	for record in records:
		message_id = record.get("messageId", "unknown")
		try:
			process_sqs_record(record, topic_arn)
			processed_count += 1
		except Exception:
			logger.exception(
				"Failed to process SQS message",
				extra={"message_id": message_id},
			)
			if message_id != "unknown":
				failed_items.append({"itemIdentifier": message_id})
			else:
				raise

	logger.info(
		"Lambda invocation completed",
		extra={
			"request_id": request_id,
			"processed_count": processed_count,
			"failed_count": len(failed_items),
		},
	)

	return {"batchItemFailures": failed_items}


def process_sqs_record(record, topic_arn):
	"""Detect sentiment for one review and publish the result to SNS."""
	message_id = record["messageId"]
	review_text = extract_review_text(record)
	language_code = os.getenv("REVIEW_LANGUAGE_CODE", DEFAULT_LANGUAGE_CODE)

	logger.info(
		"Analyzing product review sentiment",
		extra={
			"message_id": message_id,
			"language_code": language_code,
			"character_count": len(review_text),
		},
	)

	try:
		response = comprehend_client.detect_sentiment(
			Text=review_text,
			LanguageCode=language_code,
		)
	except ClientError:
		logger.exception(
			"Amazon Comprehend detect_sentiment failed",
			extra={"message_id": message_id},
		)
		raise

	sentiment = response.get("Sentiment")
	sentiment_score = response.get("SentimentScore", {})
	if not sentiment:
		raise RuntimeError("Amazon Comprehend returned no sentiment value")

	result = {
		"messageId": message_id,
		"sentiment": sentiment,
		"sentimentScore": sentiment_score,
		"languageCode": language_code,
		"reviewText": review_text,
	}

	logger.info(
		"Sentiment detected; publishing result to SNS",
		extra={
			"message_id": message_id,
			"sentiment": sentiment,
			"topic_arn": topic_arn,
		},
	)

	try:
		publish_response = sns_client.publish(
			TopicArn=topic_arn,
			Subject=f"Product review sentiment: {sentiment}",
			Message=json.dumps(result),
		)
	except ClientError:
		logger.exception(
			"SNS publish failed",
			extra={"message_id": message_id, "topic_arn": topic_arn},
		)
		raise

	logger.info(
		"Sentiment result published successfully",
		extra={
			"message_id": message_id,
			"sns_message_id": publish_response.get("MessageId"),
		},
	)


def extract_review_text(record):
	"""Extract and validate the product-review text from an SQS record."""
	message_id = record["messageId"]
	review_text = record.get("body")

	if not isinstance(review_text, str) or not review_text.strip():
		raise ValueError(f"SQS message {message_id} contains no review text")

	review_text = review_text.strip()
	review_size_bytes = len(review_text.encode("utf-8"))
	if review_size_bytes > MAX_REVIEW_BYTES:
		raise ValueError(
			f"SQS message {message_id} exceeds the Comprehend limit of "
			f"{MAX_REVIEW_BYTES} UTF-8 bytes"
		)

	return review_text

