"""Using hospital seed csv fetch MRF URLs then upload hospital name, CCN, and MRF URL to S3."""

import os
from datetime import datetime, timezone

import requests
from loguru import logger
import polars as pl
from dotenv import load_dotenv
from ingestion.conns import get_s3_client, load_hospital_seed

load_dotenv()

# AWS
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_PREFIX = os.getenv("S3_PREFIX")

def fetch_and_upload_mrf_urls(hospitals: pl.DataFrame, s3_client) -> None:
    """Fetch cms-hpt.txt for each unique hospital domain and upload raw content to S3."""
    unique_urls = hospitals["hospital_url"].unique().to_list()
    
    for url in unique_urls:
        txt_url = f"{url}/cms-hpt.txt"
        logger.info(f"Fetching {txt_url}...")
        
        headers = {
            "Accept": "text/plain, text/*, */*",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        }
        try:
            response = requests.get(txt_url, headers=headers, timeout=10)
            response.raise_for_status()
        except requests.exceptions.Timeout:
            logger.error(f"Timeout fetching {txt_url}")
            continue
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch {txt_url}: {e}")
            continue
                
        domain = url.replace("https://", "").replace("http://", "").replace("www.", "")
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        key = f"{S3_PREFIX}/bronze/mrf_discovery/{domain}_{timestamp}.txt"
        
        s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=key, Body=response.text)
        logger.info(f"Uploaded {domain} MRF index to s3://{S3_BUCKET_NAME}/{key}")


def main():
    hospitals = load_hospital_seed()
    s3_client = get_s3_client()
    fetch_and_upload_mrf_urls(hospitals, s3_client)


if __name__ == "__main__":
    main()