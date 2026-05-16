"""Using mrf index json file from s3, pull MRF data from hospital websites and send directly to S3."""

import os
import json
from urllib.parse import urlparse

import polars as pl
import requests
from dotenv import load_dotenv
from loguru import logger

from ingestion.conns import S3_BUCKET_NAME, S3_PREFIX, get_s3_client

load_dotenv()


def fetch_mrf_index_from_s3(s3_client) -> pl.DataFrame:
    """Fetch the MRF index from S3."""
    response = s3_client.list_objects_v2(
        Bucket=S3_BUCKET_NAME, Prefix=f"{S3_PREFIX}/bronze/mrf_matches/"
    )
    if "Contents" not in response:
        logger.error(
            f"No MRF index files found in s3://{S3_BUCKET_NAME}/{S3_PREFIX}/bronze/mrf_matches/"
        )
        return pl.DataFrame()
    mrf_index = response.get("Contents", [])
    mrf_index.sort(key=lambda x: x["LastModified"], reverse=True)
    mrf_index_data = (
        s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=mrf_index[0]["Key"])["Body"]
        .read()
        .decode("utf-8")
    )
    logger.info(f"Loaded MRF index from s3://{S3_BUCKET_NAME}/{mrf_index[0]['Key']}")
    mrf_index_df = json.loads(mrf_index_data)
    return pl.DataFrame(mrf_index_df)


def stream_to_s3(mrf_url: str, ccn: str, s3_client) -> bool:
    """Stream MRF file directly from hospital URL to S3 bronze layer without filtering."""
    headers = {
        "Accept": "*/*",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    }
    try:
        response = requests.get(mrf_url, headers=headers, timeout=30, stream=True)
        response.raise_for_status()
        ext = os.path.splitext(urlparse(mrf_url).path)[1] or ".json"
        key = f"{S3_PREFIX}/bronze/mrf_raw/{ccn}{ext}"
        s3_client.upload_fileobj(response.raw, S3_BUCKET_NAME, key)
        logger.info(f"Streamed MRF for CCN {ccn} to s3://{S3_BUCKET_NAME}/{key}")
        return True
    except requests.exceptions.Timeout:
        logger.error(f"Timeout fetching {mrf_url}")
        return False
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch {mrf_url}: {e}")
        return False


def main():
    s3_client = get_s3_client()
    mrf_index_df = fetch_mrf_index_from_s3(s3_client)
    if mrf_index_df.is_empty():
        logger.error("No MRF index data available, exiting.")
        return
    for row in mrf_index_df.iter_rows(named=True):
        ccn = row["ccn"]
        hospital_name = row["hospital_name"]
        mrf_url = row["mrf_url"]
        logger.info(
            f"Processing MRF for hospital '{hospital_name}' (CCN: {ccn}) from URL: {mrf_url}"
        )
        stream_to_s3(mrf_url, ccn, s3_client)


if __name__ == "__main__":
    main()
