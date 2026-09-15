import csv
import io
import json
import logging
import os
import re
from urllib.parse import unquote_plus
from typing import Any, Dict, List

import boto3
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)

s3_client = boto3.client("s3")
textract_client = boto3.client("textract")

OUTPUT_BUCKET = "dtsc-671-textract-output-fa1-2026-cnielson"


def _sanitize_extracted_text(value: str) -> str:
    """Remove decorative separators often emitted by OCR/Textract while preserving readable text."""
    cleaned = value.replace("\r", " ").replace("\n", " ")
    cleaned = re.sub(r"(?<=\w)[\|\-–—]+(?=\w)", " ", cleaned)
    cleaned = re.sub(r"[\|\-–—•·]+", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _get_table_rows(blocks: List[Dict[str, Any]]) -> List[List[str]]:
    """Convert Textract TABLE blocks into row-oriented CSV values."""
    block_map = {block.get("Id"): block for block in blocks if block.get("Id")}
    rows: List[List[str]] = []

    for block in blocks:
        if block.get("BlockType") != "TABLE":
            continue

        logger.info("Processing TABLE block with ID %s", block.get("Id"))

        for relationship in block.get("Relationships", []):
            if relationship.get("Type") != "CHILD":
                continue

            for child_id in relationship.get("Ids", []):
                child_block = block_map.get(child_id)
                if not child_block or child_block.get("BlockType") != "TABLE_ROW":
                    continue

                row_values: List[str] = []
                for row_relationship in child_block.get("Relationships", []):
                    if row_relationship.get("Type") != "CHILD":
                        continue

                    for cell_id in row_relationship.get("Ids", []):
                        cell_block = block_map.get(cell_id)
                        if not cell_block or cell_block.get("BlockType") != "CELL":
                            continue

                        cell_text = ""
                        for cell_relationship in cell_block.get("Relationships", []):
                            if cell_relationship.get("Type") != "CHILD":
                                continue
                            for grandchild_id in cell_relationship.get("Ids", []):
                                grandchild = block_map.get(grandchild_id)
                                if not grandchild:
                                    continue
                                if grandchild.get("BlockType") == "WORD":
                                    cell_text += f"{grandchild.get('Text', '')} "
                                elif grandchild.get("BlockType") == "LINE":
                                    cell_text += f"{grandchild.get('Text', '')} "
                        cleaned_text = _sanitize_extracted_text(cell_text)
                        if cleaned_text:
                            row_values.append(cleaned_text)

                if row_values:
                    rows.append(row_values)

    return rows


def _extract_name(lines: List[str]) -> str:
    """Find the likely candidate name near the top of the resume."""
    heading_markers = {
        "contact",
        "summary",
        "objective",
        "experience",
        "education",
        "skills",
        "projects",
        "certifications",
        "references",
    }

    for line in lines:
        if not line:
            continue
        normalized = line.strip()
        lowered = normalized.lower()
        if lowered in heading_markers or "@" in normalized:
            continue
        if re.fullmatch(r"[A-Za-z][A-Za-z'’.-]+(?:\s+[A-Za-z][A-Za-z'’.-]+){1,5}", normalized):
            return normalized
        tokens = normalized.split()
        if 2 <= len(tokens) <= 6 and not any(char.isdigit() for char in normalized):
            return normalized
    return ""


def _extract_email(lines: List[str]) -> str:
    """Find the first email address in the resume text."""
    pattern = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
    for line in lines:
        match = pattern.search(line)
        if match:
            return match.group(0)
    return ""


def _normalize_skills(raw_skills: str) -> str:
    """Convert OCR/PDF list separators into a clean CSV-friendly comma-separated list."""
    if not raw_skills:
        return ""

    cleaned = raw_skills.replace("\r", " ").replace("\n", " ")
    cleaned = re.sub(r"\s*[,;|/]+\s*", ", ", cleaned)
    cleaned = re.sub(r"\s*[-–—]+\s*(?=[A-Za-z0-9])", ", ", cleaned)
    cleaned = re.sub(r"\s*•\s*|\s*·\s*", ", ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" ,;")

    items = []
    for item in cleaned.split(","):
        value = item.strip(" -|;•·/")
        if value and value.lower() not in {"skills", "technical skills", "core competencies", "tools", "technologies"}:
            items.append(value)

    return ", ".join(dict.fromkeys(items))


def _extract_skills(lines: List[str]) -> str:
    """Extract a skill list from a skills section and normalize it into comma-separated values."""
    skill_lines: List[str] = []
    collecting = False
    section_markers = {
        "skills",
        "technical skills",
        "core competencies",
        "tools",
        "technologies",
        "programming languages",
    }
    stop_markers = {
        "experience",
        "education",
        "projects",
        "certifications",
        "summary",
        "objective",
        "contact",
        "references",
        "languages",
        "interests",
    }

    for line in lines:
        lowered = line.lower().strip()
        if not lowered:
            continue

        if any(marker in lowered for marker in section_markers):
            collecting = True
            continue

        if collecting:
            if any(marker in lowered for marker in stop_markers):
                break
            if lowered not in {"skills", "technical skills", "core competencies", "tools", "technologies"}:
                skill_lines.append(line)

    if skill_lines:
        return _normalize_skills(" ".join(skill_lines))

    combined_text = " ".join(lines)
    return _normalize_skills(combined_text)


def _convert_textract_response_to_csv(textract_response: Dict[str, Any]) -> str:
    """Create a compact resume CSV with only Name, Email, and Skills columns."""
    blocks = textract_response.get("Blocks", [])
    logger.info("Textract returned %s blocks", len(blocks))

    text_lines: List[str] = []
    for block in blocks:
        if block.get("BlockType") == "LINE":
            text = _sanitize_extracted_text(block.get("Text", ""))
            if text:
                text_lines.append(text)

    if not text_lines:
        logger.warning("No extractable text found in the document.")
        csv_buffer = io.StringIO()
        writer = csv.writer(csv_buffer)
        writer.writerow(["Name", "Email", "Skills"])
        writer.writerow(["", "", ""])
        return csv_buffer.getvalue()

    name_value = _extract_name(text_lines)
    email_value = _extract_email(text_lines)
    skill_value = _extract_skills(text_lines)

    logger.info("Extracted resume fields: name=%s email=%s skills=%s", name_value, email_value, skill_value)

    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerow(["Name", "Email", "Skills"])
    writer.writerow([name_value, email_value, skill_value])
    return csv_buffer.getvalue()


def _safe_url_decode(value: str) -> str:
    return unquote_plus(value)


def _get_output_key(source_key: str) -> str:
    directory, file_name = os.path.split(source_key)
    base_name, file_extension = os.path.splitext(file_name)
    if file_extension.lower() != ".pdf":
        raise ValueError(f"Unsupported input file type '{file_extension}' for key '{source_key}'. Only .pdf is supported.")
    output_name = f"{base_name}.csv"
    return os.path.join(directory, output_name)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Lambda entry point triggered by S3 object creation events."""
    logger.info("Lambda invocation started. Event=%s", json.dumps(event, default=str))

    try:
        records = event.get("Records", [])
        if not records:
            raise ValueError("No S3 records found in the event payload.")

        bucket_name = records[0].get("s3", {}).get("bucket", {}).get("name")
        source_key = records[0].get("s3", {}).get("object", {}).get("key")

        if not bucket_name:
            raise ValueError("S3 bucket name missing from event payload.")
        if not source_key:
            raise ValueError("S3 object key missing from event payload.")

        source_key = _safe_url_decode(source_key)
        logger.info("Processing S3 object: bucket=%s key=%s", bucket_name, source_key)

        target_bucket = OUTPUT_BUCKET
        pdf_object = s3_client.get_object(Bucket=bucket_name, Key=source_key)
        pdf_bytes = pdf_object["Body"].read()
        logger.info("Downloaded %s bytes from S3 for %s", len(pdf_bytes), source_key)

        response = textract_client.analyze_document(
            Document={"Bytes": pdf_bytes},
            FeatureTypes=["TABLES", "FORMS"],
        )
        logger.info("Textract analyze_document completed successfully for %s", source_key)

        csv_content = _convert_textract_response_to_csv(response)
        output_key = _get_output_key(source_key)

        s3_client.put_object(
            Bucket=target_bucket,
            Key=output_key,
            Body=csv_content.encode("utf-8"),
            ContentType="text/csv",
        )

        logger.info(
            "CSV uploaded successfully to S3 bucket=%s key=%s",
            target_bucket,
            output_key,
        )

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "PDF processed successfully via Amazon Textract.",
                    "sourceBucket": bucket_name,
                    "sourceKey": source_key,
                    "outputBucket": target_bucket,
                    "outputKey": output_key,
                }
            ),
        }

    except (ClientError, BotoCoreError) as exc:
        logger.exception("AWS SDK error while processing S3/Textract request: %s", exc)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "AWS service error", "details": str(exc)}),
        }
    except ValueError as exc:
        logger.exception("Validation error: %s", exc)
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Validation error", "details": str(exc)}),
        }
    except Exception as exc:  # pragma: no cover - final safety net
        logger.exception("Unexpected error occurred in Lambda handler: %s", exc)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Unexpected processing error", "details": str(exc)}),
        }
