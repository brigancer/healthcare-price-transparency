"""Connection utilities for AWS and data processing. """
import os
from pathlib import Path

import boto3
import polars as pl
from dotenv import load_dotenv

load_dotenv()
# AWS
AWS_PROFILE = os.getenv("AWS_PROFILE")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_PREFIX = os.getenv("S3_PREFIX")


def get_boto3_session():
    """Create and return a boto3 session using the specified AWS profile and region."""
    session = boto3.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION)
    return session


def get_s3_client():
    """Create and return an S3 client using the specified AWS profile and region."""
    session = get_boto3_session()
    s3_client = session.client("s3")
    return s3_client


def load_hospital_seed() -> pl.DataFrame:
    """Load the full Nashville MSA hospital seed file."""
    seed_path = (
        Path(__file__).parent.parent
        / "dbt_hpt_pipeline/seeds"
        / "nashville_msa_hospitals.csv"
    )
    return pl.read_csv(seed_path)


def load_hospital_ccn_list() -> list[str]:
    """Return a list of CCNs from the Nashville MSA hospital seed file."""
    return load_hospital_seed()["ccn"].cast(pl.Utf8).to_list()