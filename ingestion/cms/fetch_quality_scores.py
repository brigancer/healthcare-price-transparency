"""
Fetch hospital quality scores for Nashville MSA hospitals from the CMS
Provider Data API and upload the raw response to S3 bronze layer.

Source: https://data.cms.gov/provider-data/dataset/xubh-q36u
Cadence: Quarterly (run on Dagster schedule)
Output: s3://de-october-individual-folders/Brigance/hpt-pipeline/bronze/cms_quality/
"""

import json
from loguru import logger
from datetime import datetime, timezone
import os

import requests
from dotenv import load_dotenv
from ingestion.conns import get_s3_client, load_hospital_ccn_list

load_dotenv()
# AWS
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_PREFIX = os.getenv("S3_PREFIX")

# CMS API
CMS_API_BASE_URL = os.getenv("CMS_API_BASE_URL")


def fetch_quality_scores(ccns: list[str]) -> list[dict]:
    """Fetch quality scores for the specified list of hospital CCNs from the CMS Provider Data API."""
    logger.info(f"Fetching quality scores for {len(ccns)} hospitals from CMS API...")
    url = f"{CMS_API_BASE_URL}/xubh-q36u/0"
    payload = {
        "conditions": [{"property": "facility_id", "value": ccns, "operator": "in"}],
        "limit": 100,
    }
    response = requests.post(url, json=payload)
    response.raise_for_status()
    results = response.json().get("results", [])
    logger.info(f"Retrieved {len(results)} quality scores from CMS API.")
    return results


def upload_to_s3(data: list[dict], s3_client) -> None:
    """Upload the given data to S3 as a JSON file in the bronze layer with a timestamped filename."""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    key = f"{S3_PREFIX}/bronze/cms_quality/quality_scores_{timestamp}.json"
    body = json.dumps(data, indent=2)
    s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=key, Body=body)
    logger.info(f"Uploaded quality scores to s3://{S3_BUCKET_NAME}/{key}")


def main():
    ccns = load_hospital_ccn_list()
    s3_client = get_s3_client()
    quality_scores = fetch_quality_scores(ccns)
    upload_to_s3(quality_scores, s3_client)


if __name__ == "__main__":
    main()
