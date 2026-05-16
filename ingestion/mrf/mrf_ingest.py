"""Using mrf index json file from s3, pull MRF data from hospital websites and upload to S3."""

import io
import json
import os
import pathlib
import tempfile
import zipfile

import ijson
import polars as pl
import pyarrow as pa
import pyarrow.csv as pa_csv
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


def _read_csv_bytes(content: bytes, shoppable_codes: list[str]) -> pl.DataFrame:
    """Write CSV bytes to a temp file, read with PyArrow skipping 2 metadata rows,
    filter to shoppable codes, and return a Polars DataFrame."""
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
        tmp.write(content)
        tmp_path = tmp.name
    try:
        read_options = pa_csv.ReadOptions(skip_rows=2)
        table = pa_csv.read_csv(
            tmp_path,
            read_options=read_options,
            convert_options=pa_csv.ConvertOptions(
                column_types={"code|1": pa.large_utf8()}
            ),
        )
        return (
            pl.from_arrow(table)
            .with_columns(pl.col("code|1").cast(pl.Utf8))
            .filter(pl.col("code|1").is_in(shoppable_codes))
        )
    finally:
        os.remove(tmp_path)


def stream_and_filter_mrf_file(
    mrf_url: str, shoppable_services: pl.DataFrame, ccn: str
) -> pl.DataFrame:
    """stream the mrf file from the url determine if zipped and file type and use polars lazy loading to read data in chunks, filter against shoppable services csv in seeds, and return a json of hospital ccn, hospital name, and prices for shoppable services."""
    headers = {
        "Accept": "*/*",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    }
    try:
        response = requests.get(mrf_url, headers=headers, timeout=30, stream=True)
        response.raise_for_status()
        url_path = mrf_url.split("?")[0]
        is_zipped = url_path.endswith(".zip")
        shoppable_codes = shoppable_services["code"].to_list()
        if is_zipped:
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                inner_files = z.namelist()
                csv_files = [
                    f for f in inner_files if f.endswith(".csv") and "/" not in f
                ]
                json_files = [
                    f for f in inner_files if f.endswith(".json") and "/" not in f
                ]
                if csv_files:
                    with z.open(csv_files[0]) as f:
                        mrf_df = _read_csv_bytes(f.read(), shoppable_codes)
                elif json_files:
                    with z.open(json_files[0]) as f:
                        mrf_df = pl.read_json(f)
                else:
                    logger.error(f"No CSV or JSON files found in ZIP from {mrf_url}")
                    return pl.DataFrame()
        else:
            if url_path.endswith(".json"):
                matching_records = []
                for record in ijson.items(
                    response.raw, "standard_charge_information.item"
                ):
                    codes = [c["code"] for c in record.get("code_information", [])]
                    if any(code in shoppable_codes for code in codes):
                        matching_records.append(record)
                mrf_df = pl.DataFrame(matching_records)
            elif url_path.endswith(".csv"):
                mrf_df = _read_csv_bytes(response.content, shoppable_codes)
            else:
                logger.error(f"Unknown file format for {mrf_url}")
                return pl.DataFrame()
        return mrf_df.with_columns(pl.lit(ccn).alias("ccn"))
    except requests.exceptions.Timeout:
        logger.error(f"Timeout fetching {mrf_url}")
        return pl.DataFrame()
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch {mrf_url}: {e}")
        return pl.DataFrame()


def upload_filtered_mrf(mrf_df: pl.DataFrame, ccn: str):
    """Upload the filtered MRF data to S3 as a JSON file."""
    if mrf_df.is_empty():
        logger.info(
            f"No shoppable services found for hospital CCN {ccn}, skipping upload."
        )
        return
    key = f"{S3_PREFIX}/bronze/mrf/{ccn}.json"
    s3_client = get_s3_client()
    s3_client.put_object(
        Bucket=S3_BUCKET_NAME, Key=key, Body=mrf_df.write_json().encode("utf-8")
    )
    logger.info(
        f"Uploaded filtered MRF data for CCN {ccn} to s3://{S3_BUCKET_NAME}/{key}"
    )


def main():
    s3_client = get_s3_client()
    mrf_index_df = fetch_mrf_index_from_s3(s3_client)
    if mrf_index_df.is_empty():
        logger.error("No MRF index data available, exiting.")
        return
    shoppable_services = pl.read_csv(
        pathlib.Path(__file__).parent.parent.parent
        / "dbt_hpt_pipeline/seeds/shoppable_services.csv"
    ).select(pl.col("code").cast(pl.Utf8))
    for row in mrf_index_df.iter_rows(named=True):
        ccn = row["ccn"]
        hospital_name = row["hospital_name"]
        mrf_url = row["mrf_url"]
        logger.info(
            f"Processing MRF for hospital '{hospital_name}' (CCN: {ccn}) from URL: {mrf_url}"
        )
        mrf_df = stream_and_filter_mrf_file(mrf_url, shoppable_services, ccn)
        upload_filtered_mrf(mrf_df, ccn)


if __name__ == "__main__":
    main()
