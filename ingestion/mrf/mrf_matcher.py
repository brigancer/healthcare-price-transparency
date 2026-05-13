"""Read MRF discovery files from S3, and match to seed hospitals"""
import json
import os
import re
from datetime import datetime, timezone

import numpy as np
import polars as pl
import requests
from dotenv import load_dotenv
from loguru import logger
from rapidfuzz import fuzz
from scipy.optimize import linear_sum_assignment

from ingestion.conns import get_s3_client, load_hospital_seed

load_dotenv()

# AWS
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_PREFIX = os.getenv("S3_PREFIX")

def parse_cms_hpt_txt(content: str, domain: str) -> pl.DataFrame:
    """Parse cms-hpt.txt content to extract MRF URLs with associated hospital names."""
    mrf_urls = []
    current_hospital = {}
    blocks = content.split("\n\n")
    for block in blocks:
        for line in block.splitlines():
            if line.startswith("location-name:"):
                current_hospital["location-name"] = line.split("location-name:")[1].strip()
            elif line.startswith("mrf-url:"):
                current_hospital["mrf-url"] = line.split("mrf-url:")[1].strip()
                try:
                    mrf_urls.append((domain, current_hospital["location-name"], current_hospital["mrf-url"]))
                except KeyError:
                    logger.warning(f"Skipping incomplete hospital data: {current_hospital}")
                current_hospital = {}
    return pl.DataFrame(mrf_urls, schema=["domain", "location_name", "mrf_url"], orient="row")

def build_hospital_name_matrix(mrf_urls: pl.DataFrame, hospitals: pl.DataFrame) -> dict:
    """Fetch MRF URLs for each hospital from each unique domain."""
    hospital_urls = (
        hospitals["hospital_url"]
        .str.replace("https://", "")
        .str.replace("http://", "")
        .str.replace("www.", "")
        .unique()
        .to_list()
    )
    hospital_name_dict = {}
    for url in hospital_urls:
        mrf_candidates = mrf_urls.filter(pl.col("domain") == url)
        mrf_names = mrf_candidates["location_name"].to_list()
        hospital_candidates = hospitals.filter(
            pl.col("hospital_url").str.replace("https://", "").str.replace("http://", "").str.replace("www.", "") == url
        )
        hospital_names = hospital_candidates["hospital_name"].to_list()
        hospital_ccns = hospital_candidates["ccn"].to_list()
        hospital_name_matrix = np.zeros((len(hospital_names), len(mrf_names)), dtype=float)
        for i, h_name in enumerate(hospital_names):
            for j, m_name in enumerate(mrf_names):
                hospital_name_matrix[i, j] = fuzz.token_set_ratio(h_name.upper(), m_name.upper())
        hospital_name_dict[url] = (hospital_ccns, hospital_names, mrf_names, hospital_name_matrix)
    return hospital_name_dict
    
def hospital_mrf_matcher(hospitals: pl.DataFrame, hospital_name_dict: dict, mrf_urls: pl.DataFrame) -> pl.DataFrame:
    """Use the hospital name matrix to find the best MRF URL match for each hospital."""
    # Build a lookup dictionary for location_name -> mrf_url
    location_to_url = dict(zip(mrf_urls["location_name"].to_list(), mrf_urls["mrf_url"].to_list()))
    matched_hospitals = []
    for url, (hospital_ccns, hospital_names, mrf_names, hospital_name_matrix) in hospital_name_dict.items():
        if hospital_name_matrix.size == 0:
            logger.warning(f"No candidates found for domain: {url}")
            continue
        row_ind, col_ind = linear_sum_assignment(-hospital_name_matrix) 
        for i, j in zip(row_ind, col_ind):
            if hospital_name_matrix[i, j] >= 80:  # Threshold for a good match
                try:
                    mrf_url = location_to_url.get(mrf_names[j])
                    if mrf_url is not None:
                        matched_hospitals.append((hospital_ccns[i], hospital_names[i], mrf_names[j], mrf_url))
                    else:
                        logger.error(f"MRF URL not found for match '{mrf_names[j]}'")
                except Exception as e:
                    logger.error(f"Error retrieving MRF URL for match '{mrf_names[j]}': {e}")
            else:
                logger.info(f"No good match for hospital '{hospital_names[i]}' with MRF name '{mrf_names[j]}' (score: {hospital_name_matrix[i, j]})")
    return pl.DataFrame(matched_hospitals, schema=["ccn", "hospital_name", "location_name", "mrf_url"], orient="row")

def import_cms_hpt_txt(s3_client) -> pl.DataFrame:
    """Import all cms-hpt.txt files from S3 and parse to extract MRF URLs."""
    response = s3_client.list_objects_v2(
        Bucket=S3_BUCKET_NAME,
        Prefix=f"{S3_PREFIX}/bronze/mrf_discovery/"
    )
    files = response.get("Contents", [])
    files.sort(key=lambda x: x["LastModified"], reverse=True)
    seen_domains = {}
    for file in files:
        domain = file["Key"].split("/")[-1].split("_")[0]
        if domain not in seen_domains:
            seen_domains[domain] = file["Key"]
    mrf_dfs = []
    for domain, key in seen_domains.items():
        logger.info(f"Processing MRF discovery file for domain: {domain}")
        try:
            obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=key)
            content = obj["Body"].read().decode("utf-8")
            mrf_df = parse_cms_hpt_txt(content, domain)
            mrf_dfs.append(mrf_df)
        except Exception as e:
            logger.error(f"Error processing file {key}: {e}")
    return pl.concat(mrf_dfs) if mrf_dfs else pl.DataFrame(schema=["domain", "location_name", "mrf_url"])
    
def main():
    hospitals = load_hospital_seed()
    s3_client = get_s3_client()
    imported_mrf_urls = import_cms_hpt_txt(s3_client)
    hospital_name_dict = build_hospital_name_matrix(imported_mrf_urls, hospitals)
    matched_hospitals = hospital_mrf_matcher(hospitals, hospital_name_dict, imported_mrf_urls)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    s3_client.put_object(
        Bucket=S3_BUCKET_NAME, 
        Key=f"{S3_PREFIX}/bronze/mrf_matches/nashville_msa_hospitals_mrf_index_{timestamp}.json", 
        Body=matched_hospitals.write_json()
    )
    existing = s3_client.list_objects_v2(
        Bucket=S3_BUCKET_NAME,
        Prefix=f"{S3_PREFIX}/bronze/mrf_matches/nashville_msa_hospitals_mrf_index"
        ).get("Contents", [])
    existing.sort(key=lambda x: x["LastModified"], reverse=True)
    for old in existing[3:]:
        s3_client.delete_object(Bucket=S3_BUCKET_NAME, Key=old["Key"])
        logger.info(f"Deleted old MRF match file: {old['Key']}")
    logger.info(f"Uploaded matched hospitals to s3://{S3_BUCKET_NAME}/{S3_PREFIX}/bronze/mrf_matches/nashville_msa_hospitals_mrf_index_{timestamp}.json")
    
if __name__ == "__main__":
    main()