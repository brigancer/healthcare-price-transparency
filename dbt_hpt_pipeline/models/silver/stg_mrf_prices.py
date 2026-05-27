import io
import gzip
import json as json_lib
import logging
import re
import subprocess
import zipfile

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


FIXED_COLS = {
    "description",
    "code|1",
    "code|1|type",
    "code|2",
    "code|2|type",
    "code|3",
    "code|3|type",
    "code|4",
    "code|4|type",
    "billing_class",
    "setting",
    "drug_unit_of_measurement",
    "drug_type_of_measurement",
    "modifiers",
    "standard_charge|gross",
    "standard_charge|discounted_cash",
    "standard_charge|min",
    "standard_charge|max",
    "additional_generic_notes",
    "ccn",
}

FIXED_COLS_RENAME = {
    "code|1": "code_1",
    "code|1|type": "code_1_type",
    "code|2": "code_2",
    "code|2|type": "code_2_type",
    "code|3": "code_3",
    "code|3|type": "code_3_type",
    "code|4": "code_4",
    "code|4|type": "code_4_type",
    "standard_charge|gross": "standard_charge_gross",
    "standard_charge|discounted_cash": "standard_charge_discounted_cash",
    "standard_charge|min": "standard_charge_min",
    "standard_charge|max": "standard_charge_max",
}

STRING_COLS = [
    "ccn",
    "description",
    "code_1",
    "code_1_type",
    "code_2",
    "code_2_type",
    "code_3",
    "code_3_type",
    "code_4",
    "code_4_type",
    "billing_class",
    "setting",
    "modifiers",
    "drug_unit_of_measurement",
    "drug_type_of_measurement",
    "additional_generic_notes",
    "payer_name",
    "plan_name",
    "negotiated_algorithm",
    "methodology",
    "additional_payer_notes",
]

NUMERIC_COLS = [
    "negotiated_dollar",
    "negotiated_percentage",
    "standard_charge_gross",
    "standard_charge_discounted_cash",
    "standard_charge_min",
    "standard_charge_max",
    "median_amount",
    "10th_percentile",
    "90th_percentile",
    "count",
]


def list_volume_files(spark, path: str) -> list[str]:
    """List all files in the bronze volume."""
    files_df = (
        spark.read.format("binaryFile")
        .option("pathGlobFilter", "*")
        .option("recursiveFileLookup", "false")
        .load(path)
    )
    return [row.path for row in files_df.select("path").collect()]


def read_magic_bytes(spark, file_path: str) -> bytes:
    """Read first 4 bytes of a file for compression detection."""
    row = (
        spark.read.format("binaryFile")
        .load(file_path)
        .select(F.col("content").substr(1, 4).alias("magic"))
        .first()
    )
    return row.magic


def read_file_bytes(spark, file_path: str) -> bytes:
    """Read a single file from the volume into memory as bytes."""
    row = spark.read.format("binaryFile").load(file_path).select("content").first()
    return row.content


def detect_compression(file_bytes: bytes) -> str:
    """Detect compression type by inspecting magic bytes."""
    if file_bytes[:2] == b"\x1f\x8b":
        return "gzip"
    if file_bytes[:4] == b"PK\x03\x04":
        return "zip"
    return "none"


def detect_csv_shape(mrf_raw_prices: DataFrame) -> str:
    """
    Detect whether a CSV DataFrame is wide or long format.
    Wide: payer columns follow metric|payer|plan pattern.
    Long: has explicit payer_name column.
    """
    col_names = set(mrf_raw_prices.columns)
    if "payer_name" in col_names:
        return "long"
    payer_pattern_cols = [
        col_name
        for col_name in col_names
        if col_name.count("|") >= 2 and col_name not in FIXED_COLS
    ]
    if payer_pattern_cols:
        return "wide"
    return "unknown"


def extract_ccn(file_path: str) -> str:
    """Extract 6-digit CCN from file path."""
    match = re.search(r"/([0-9]{6})\.[a-z]+$", file_path)
    return match.group(1) if match else "unknown"


def extract_json_field_names(mrf_json_prices: DataFrame) -> tuple:
    """
    Dynamically extract field names from nested JSON schema.
    Returns (charge_setting_fields, payer_fields) as sets of strings.
    """
    schema_dict = json_lib.loads(mrf_json_prices.schema.json())

    sci_fields = next(
        field
        for field in schema_dict["fields"]
        if field["name"] == "standard_charge_information"
    )
    sci_element_fields = sci_fields["type"]["elementType"]["fields"]

    sc_fields = next(
        field for field in sci_element_fields if field["name"] == "standard_charges"
    )
    charge_setting_fields = {
        field["name"] for field in sc_fields["type"]["elementType"]["fields"]
    }

    pi_fields = next(
        field
        for field in sc_fields["type"]["elementType"]["fields"]
        if field["name"] == "payers_information"
    )
    payer_fields = {
        field["name"] for field in pi_fields["type"]["elementType"]["fields"]
    }

    return charge_setting_fields, payer_fields


def decompress_zip(file_bytes: bytes) -> str:
    """Extract CSV from a ZIP archive and return contents as string."""
    with zipfile.ZipFile(io.BytesIO(file_bytes)) as zf:
        csv_files = [
            name for name in zf.namelist() if name.endswith(".csv") and "/" not in name
        ]
        if not csv_files:
            raise ValueError("No CSV found inside ZIP archive")
        return zf.read(csv_files[0]).decode("utf-8")


def read_csv_from_string(spark, content: str, ccn: str) -> DataFrame:
    """
    Parse a CSV string into a Spark DataFrame using Polars.
    Uses StringIO to avoid writing temp files.
    Skips 2 metadata rows, keeps only ASCII columns.
    """
    import polars as pl

    mrf_raw_prices = spark.createDataFrame(
        pl.read_csv(io.StringIO(content), skip_rows=2, infer_schema=False).to_pandas()
    )
    clean_col_names = [
        col_name
        for col_name in mrf_raw_prices.columns
        if col_name.isascii() and not col_name.startswith("_")
    ]
    mrf_raw_prices = mrf_raw_prices.select(clean_col_names).replace("", None)
    return mrf_raw_prices.withColumn("ccn", F.lit(ccn))


def read_plain_csv(spark, file_path: str, ccn: str) -> DataFrame:
    """Read a plain uncompressed CSV file."""
    mrf_raw_prices = (
        spark.read.format("csv")
        .option("header", "true")
        .option("skipRows", 2)
        .option("inferSchema", "true")
        .load(file_path)
    )
    clean_col_names = [
        col_name
        for col_name in mrf_raw_prices.columns
        if col_name.isascii() and not col_name.startswith("_")
    ]
    mrf_raw_prices = mrf_raw_prices.select(clean_col_names).replace("", None)
    return mrf_raw_prices.withColumn("ccn", F.lit(ccn))


def read_json(spark, file_path: str, ccn: str) -> DataFrame:
    """Read a JSON MRF file using multiLine mode."""
    mrf_raw_prices = spark.read.json(file_path, multiLine=True)
    return mrf_raw_prices.withColumn("ccn", F.lit(ccn))


def read_gzip_json(spark, file_path: str, ccn: str) -> DataFrame:
    """Read a gzip-compressed JSON MRF file using Spark native decompression."""
    mrf_raw_prices = spark.read.option("compression", "gzip").json(
        file_path, multiLine=True
    )
    return mrf_raw_prices.withColumn("ccn", F.lit(ccn))


def transform_wide_csv(wide_mrf_prices: DataFrame) -> DataFrame:
    """
    Unpivot a wide CSV DataFrame into canonical Silver schema.
    Each service row becomes one row per payer/plan combination.
    CMS MRF spec defines 9 metric columns per payer/plan combo.
    Pipe-delimited column names renamed to underscore format
    to comply with Delta Lake column name requirements.
    """
    payer_cols = [
        col_name for col_name in wide_mrf_prices.columns if col_name not in FIXED_COLS
    ]

    payer_plan_keys = set()
    for col_name in payer_cols:
        parts = col_name.split("|")
        if len(parts) >= 3:
            payer_plan_keys.add(f"{parts[1]}|{parts[2]}")

    if not payer_plan_keys:
        raise ValueError(
            "No payer/plan combos detected in wide CSV — check source file format"
        )

    service_cols = [
        col_name for col_name in wide_mrf_prices.columns if col_name in FIXED_COLS
    ]

    payer_slices = []
    for key in payer_plan_keys:
        payer_name, plan_name = key.split("|", 1)

        select_exprs = []
        for raw_name in service_cols:
            canonical_name = FIXED_COLS_RENAME.get(raw_name, raw_name)
            select_exprs.append(F.col(f"`{raw_name}`").alias(canonical_name))

        select_exprs.append(F.lit(payer_name).alias("payer_name"))
        select_exprs.append(F.lit(plan_name).alias("plan_name"))

        col_map = {
            "negotiated_dollar": f"standard_charge|{key}|negotiated_dollar",
            "negotiated_percentage": f"standard_charge|{key}|negotiated_percentage",
            "negotiated_algorithm": f"standard_charge|{key}|negotiated_algorithm",
            "methodology": f"standard_charge|{key}|methodology",
            "median_amount": f"median_amount|{key}",
            "10th_percentile": f"10th_percentile|{key}",
            "90th_percentile": f"90th_percentile|{key}",
            "count": f"count|{key}",
            "additional_payer_notes": f"additional_payer_notes|{key}",
        }

        for canonical_name, raw_col_name in col_map.items():
            if raw_col_name in wide_mrf_prices.columns:
                select_exprs.append(F.col(f"`{raw_col_name}`").alias(canonical_name))
            else:
                select_exprs.append(F.lit(None).alias(canonical_name))

        payer_slices.append(wide_mrf_prices.select(select_exprs))

    mrf_payer_charges = payer_slices[0]
    for payer_slice in payer_slices[1:]:
        mrf_payer_charges = mrf_payer_charges.unionByName(
            payer_slice, allowMissingColumns=True
        )

    for col_name in NUMERIC_COLS:
        mrf_payer_charges = mrf_payer_charges.withColumn(
            col_name, F.expr(f"TRY_CAST(`{col_name}` AS DOUBLE)")
        )

    return mrf_payer_charges


def transform_long_csv(long_mrf_prices: DataFrame) -> DataFrame:
    """
    Map a long format CSV DataFrame to canonical Silver schema.
    Already one row per payer/plan — dynamically maps columns
    that are present, fills null for missing optional columns.
    """
    col_names = set(long_mrf_prices.columns)

    def get_col(raw_name: str, canonical_name: str):
        """Return column expression — mapped if present, null if missing."""
        if raw_name in col_names:
            return F.col(f"`{raw_name}`").alias(canonical_name)
        return F.lit(None).cast("string").alias(canonical_name)

    if "additional_payer_notes" in col_names:
        payer_notes_expr = F.col("additional_payer_notes")
    elif "additionalpayernotes" in col_names:
        payer_notes_expr = F.col("additionalpayernotes")
    else:
        payer_notes_expr = F.lit(None).cast("string")

    mrf_payer_charges = long_mrf_prices.select(
        F.col("ccn"),
        F.col("description"),
        get_col("code|1", "code_1"),
        get_col("code|1|type", "code_1_type"),
        get_col("code|2", "code_2"),
        get_col("code|2|type", "code_2_type"),
        get_col("code|3", "code_3"),
        get_col("code|3|type", "code_3_type"),
        get_col("code|4", "code_4"),
        get_col("code|4|type", "code_4_type"),
        get_col("billing_class", "billing_class"),
        F.col("setting"),
        F.col("modifiers"),
        F.col("drug_unit_of_measurement"),
        F.col("drug_type_of_measurement"),
        get_col("standard_charge|gross", "standard_charge_gross"),
        get_col("standard_charge|discounted_cash", "standard_charge_discounted_cash"),
        get_col("standard_charge|min", "standard_charge_min"),
        get_col("standard_charge|max", "standard_charge_max"),
        get_col("additional_generic_notes", "additional_generic_notes"),
        F.col("payer_name"),
        F.col("plan_name"),
        get_col("standard_charge|negotiated_dollar", "negotiated_dollar"),
        get_col("standard_charge|negotiated_percentage", "negotiated_percentage"),
        get_col("standard_charge|negotiated_algorithm", "negotiated_algorithm"),
        get_col("standard_charge|methodology", "methodology"),
        payer_notes_expr.alias("additional_payer_notes"),
        get_col("median_amount", "median_amount"),
        get_col("10th_percentile", "10th_percentile"),
        get_col("90th_percentile", "90th_percentile"),
        get_col("count", "count"),
    )

    for col_name in NUMERIC_COLS:
        mrf_payer_charges = mrf_payer_charges.withColumn(
            col_name, F.expr(f"TRY_CAST(`{col_name}` AS DOUBLE)")
        )

    return mrf_payer_charges


def transform_json(mrf_json_prices: DataFrame) -> DataFrame:
    """
    Flatten nested JSON MRF structure to canonical Silver schema.
    Explodes three levels:
        standard_charge_information → mrf_services
        standard_charges            → mrf_charge_settings
        payers_information          → mrf_payer_charges
    Dynamically detects available fields at each level.
    Up to 4 codes extracted — missing indices return null safely.
    """
    charge_setting_fields, payer_fields = extract_json_field_names(mrf_json_prices)

    def get_charge_field(raw_name: str, canonical_name: str):
        """Return charge_setting field — mapped if present, null if missing."""
        if raw_name in charge_setting_fields:
            return F.col(f"charge_setting.{raw_name}").alias(canonical_name)
        return F.lit(None).cast("string").alias(canonical_name)

    def get_payer_field(raw_name: str, canonical_name: str):
        """Return payer field — mapped if present, null if missing."""
        if raw_name in payer_fields:
            return F.col(f"payer.{raw_name}").alias(canonical_name)
        return F.lit(None).cast("string").alias(canonical_name)

    mrf_services = mrf_json_prices.select(
        F.col("ccn"), F.explode("standard_charge_information").alias("service")
    ).select(
        F.col("ccn"),
        F.col("service.description").alias("description"),
        F.col("service.code_information")[0]["code"].alias("code_1"),
        F.col("service.code_information")[0]["type"].alias("code_1_type"),
        F.col("service.code_information")[1]["code"].alias("code_2"),
        F.col("service.code_information")[1]["type"].alias("code_2_type"),
        F.col("service.code_information")[2]["code"].alias("code_3"),
        F.col("service.code_information")[2]["type"].alias("code_3_type"),
        F.col("service.code_information")[3]["code"].alias("code_4"),
        F.col("service.code_information")[3]["type"].alias("code_4_type"),
        F.col("service.standard_charges").alias("standard_charges"),
    )

    mrf_charge_settings = mrf_services.select(
        F.col("ccn"),
        F.col("description"),
        F.col("code_1"),
        F.col("code_1_type"),
        F.col("code_2"),
        F.col("code_2_type"),
        F.col("code_3"),
        F.col("code_3_type"),
        F.col("code_4"),
        F.col("code_4_type"),
        F.explode("standard_charges").alias("charge_setting"),
    ).select(
        F.col("ccn"),
        F.col("description"),
        F.col("code_1"),
        F.col("code_1_type"),
        F.col("code_2"),
        F.col("code_2_type"),
        F.col("code_3"),
        F.col("code_3_type"),
        F.col("code_4"),
        F.col("code_4_type"),
        get_charge_field("setting", "setting"),
        get_charge_field("modifiers", "modifiers"),
        get_charge_field("gross_charge", "standard_charge_gross"),
        get_charge_field("discounted_cash", "standard_charge_discounted_cash"),
        get_charge_field("minimum", "standard_charge_min"),
        get_charge_field("maximum", "standard_charge_max"),
        get_charge_field("billing_class", "billing_class"),
        get_charge_field("drug_unit_of_measurement", "drug_unit_of_measurement"),
        get_charge_field("drug_type_of_measurement", "drug_type_of_measurement"),
        get_charge_field("additional_generic_notes", "additional_generic_notes"),
        F.col("charge_setting.payers_information").alias("payers_information"),
    )

    mrf_payer_charges = mrf_charge_settings.select(
        F.col("ccn"),
        F.col("description"),
        F.col("code_1"),
        F.col("code_1_type"),
        F.col("code_2"),
        F.col("code_2_type"),
        F.col("code_3"),
        F.col("code_3_type"),
        F.col("code_4"),
        F.col("code_4_type"),
        F.col("billing_class"),
        F.col("setting"),
        F.col("modifiers"),
        F.col("drug_unit_of_measurement"),
        F.col("drug_type_of_measurement"),
        F.col("standard_charge_gross"),
        F.col("standard_charge_discounted_cash"),
        F.col("standard_charge_min"),
        F.col("standard_charge_max"),
        F.col("additional_generic_notes"),
        F.explode("payers_information").alias("payer"),
    ).select(
        F.col("ccn"),
        F.col("description"),
        F.col("code_1"),
        F.col("code_1_type"),
        F.col("code_2"),
        F.col("code_2_type"),
        F.col("code_3"),
        F.col("code_3_type"),
        F.col("code_4"),
        F.col("code_4_type"),
        F.col("billing_class"),
        F.col("setting"),
        F.col("modifiers"),
        F.col("drug_unit_of_measurement"),
        F.col("drug_type_of_measurement"),
        F.col("standard_charge_gross"),
        F.col("standard_charge_discounted_cash"),
        F.col("standard_charge_min"),
        F.col("standard_charge_max"),
        F.col("additional_generic_notes"),
        get_payer_field("payer_name", "payer_name"),
        get_payer_field("plan_name", "plan_name"),
        get_payer_field("standard_charge_dollar", "negotiated_dollar"),
        get_payer_field("standard_charge_percentage", "negotiated_percentage"),
        get_payer_field("standard_charge_algorithm", "negotiated_algorithm"),
        get_payer_field("methodology", "methodology"),
        get_payer_field("additional_payer_notes", "additional_payer_notes"),
        get_payer_field("median_amount", "median_amount"),
        get_payer_field("10th_percentile", "10th_percentile"),
        get_payer_field("90th_percentile", "90th_percentile"),
        get_payer_field("count", "count"),
    )

    for col_name in NUMERIC_COLS:
        mrf_payer_charges = mrf_payer_charges.withColumn(
            col_name, F.expr(f"TRY_CAST(`{col_name}` AS DOUBLE)")
        )

    return mrf_payer_charges


def model(dbt, spark):
    dbt.config(
        materialized="incremental",
        unique_key=["ccn", "code_1", "payer_name", "plan_name", "setting"],
        on_schema_change="sync_all_columns",
        submission_method="serverless_cluster",
    )

    subprocess.run(["pip", "install", "polars>=1.40.1"], check=True)

    logging.basicConfig(level=logging.INFO)
    volume_path = "/Volumes/workspace/hpt_dev/bronze_mrf_raw"

    processed = []
    skipped = []
    failed = []

    # Determine which CCNs are already in the Silver table
    already_processed_ccns = set()
    if dbt.is_incremental:
        already_processed_ccns = {
            row.ccn
            for row in spark.sql(f"select distinct ccn from {dbt.this}").collect()
        }
        logger.info(
            f"Incremental run — skipping already processed CCNs: {already_processed_ccns}"
        )

    # List all files in bronze volume
    all_file_paths = list_volume_files(spark, volume_path)
    logger.info(f"Found {len(all_file_paths)} files in volume")

    hospital_payer_prices_list = []

    for file_path in all_file_paths:
        ccn = extract_ccn(file_path)

        if ccn in already_processed_ccns:
            logger.info(f"Skipping already processed CCN: {ccn}")
            skipped.append(ccn)
            continue

        try:
            logger.info(f"Processing CCN: {ccn}, file: {file_path}")

            # Step 1 — route by file type
            if file_path.endswith(".json"):
                mrf_raw_prices = read_json(spark, file_path, ccn)
                file_format = "json"
            else:
                magic = read_magic_bytes(spark, file_path)
                compression = detect_compression(magic)
                logger.info(f"CCN {ccn} — compression detected: {compression}")

                if compression == "gzip":
                    file_bytes = read_file_bytes(spark, file_path)
                    raw_content = gzip.decompress(file_bytes).decode("utf-8")
                    mrf_raw_prices = read_csv_from_string(spark, raw_content, ccn)
                elif compression == "zip":
                    file_bytes = read_file_bytes(spark, file_path)
                    raw_content = decompress_zip(file_bytes)
                    mrf_raw_prices = read_csv_from_string(spark, raw_content, ccn)
                else:
                    mrf_raw_prices = read_plain_csv(spark, file_path, ccn)
                file_format = "csv"

            logger.info(f"CCN {ccn} — file format: {file_format}")

            # Step 2 — detect shape and transform
            if file_format == "json":
                hospital_payer_prices = transform_json(mrf_raw_prices)
            else:
                csv_shape = detect_csv_shape(mrf_raw_prices)
                logger.info(f"CCN {ccn} — CSV shape detected: {csv_shape}")

                if csv_shape == "wide":
                    hospital_payer_prices = transform_wide_csv(mrf_raw_prices)
                elif csv_shape == "long":
                    hospital_payer_prices = transform_long_csv(mrf_raw_prices)
                else:
                    logger.warning(
                        f"CCN {ccn} — unknown CSV shape, skipping. "
                        f"Columns: {mrf_raw_prices.columns}"
                    )
                    skipped.append(ccn)
                    continue

            hospital_payer_prices_list.append(hospital_payer_prices)
            processed.append(ccn)
            logger.info(f"CCN {ccn} — successfully transformed")

        except Exception as e:
            logger.error(f"CCN {ccn} — failed with error: {str(e)}")
            failed.append(ccn)
            continue

    # Log summary
    logger.info(
        f"Processing complete — "
        f"processed: {processed}, "
        f"skipped: {skipped}, "
        f"failed: {failed}"
    )

    if not hospital_payer_prices_list:
        raise ValueError(
            f"No hospitals successfully processed. Failed: {failed}, Skipped: {skipped}"
        )

    # Union all hospital DataFrames
    unioned_hospital_payer_prices = hospital_payer_prices_list[0]
    for hospital_payer_prices in hospital_payer_prices_list[1:]:
        unioned_hospital_payer_prices = unioned_hospital_payer_prices.unionByName(
            hospital_payer_prices, allowMissingColumns=True
        )

    # Filter to shoppable services
    shoppable_df = dbt.ref("shoppable_services")

    silver_mrf_prices = unioned_hospital_payer_prices.join(
        shoppable_df,
        (unioned_hospital_payer_prices.code_1 == shoppable_df.code)
        | (unioned_hospital_payer_prices.code_2 == shoppable_df.code)
        | (unioned_hospital_payer_prices.code_3 == shoppable_df.code)
        | (unioned_hospital_payer_prices.code_4 == shoppable_df.code),
        how="inner",
    ).drop(shoppable_df.code)

    # Enforce final Silver schema types
    for col_name in STRING_COLS:
        silver_mrf_prices = silver_mrf_prices.withColumn(
            col_name, F.col(col_name).cast("string")
        )
    for col_name in NUMERIC_COLS:
        silver_mrf_prices = silver_mrf_prices.withColumn(
            col_name, F.expr(f"TRY_CAST(`{col_name}` AS DOUBLE)")
        )

    logger.info(f"Final Silver table row count: {silver_mrf_prices.count()}")

    return silver_mrf_prices
