from pathlib import Path

import dagster as dg
from dagster import AssetKey
from dagster_dbt import (
    DagsterDbtTranslator,
    DbtCliResource,
    dbt_assets,
)

from ingestion.cms.fetch_quality_scores import main as run_fetch_quality_scores
from ingestion.mrf.mrf_discovery import main as run_mrf_discovery
from ingestion.mrf.mrf_ingest import main as run_mrf_ingest
from ingestion.mrf.mrf_matcher import main as run_mrf_matcher
from transform.mrf.mrf_transform import main as run_mrf_transform


class SilverDbtTranslator(DagsterDbtTranslator):
    def get_group_name(self, dbt_resource_props):
        return "silver"


class GoldDbtTranslator(DagsterDbtTranslator):
    def get_group_name(self, dbt_resource_props):
        return "gold"

DBT_PROJECT_DIR = Path(__file__).parent.parent.parent.parent.parent / "dbt_hpt_pipeline"
DBT_MANIFEST = DBT_PROJECT_DIR / "target" / "manifest.json"


# ── Ingestion Assets ──────────────────────────────────────────────────────────

@dg.asset(
    group_name="ingestion",
    description="Fetches cms-hpt.txt MRF discovery files from each hospital system domain and uploads to S3 bronze layer.",
)
def mrf_discovery(context: dg.AssetExecutionContext) -> None:
    context.log.info("Starting MRF discovery...")
    run_mrf_discovery()
    context.log.info("MRF discovery complete.")


@dg.asset(
    group_name="ingestion",
    deps=[mrf_discovery],
    description="Fuzzy matches hospital names from MRF discovery files to Nashville MSA seed hospitals. Uploads MRF index to S3.",
)
def mrf_matcher(context: dg.AssetExecutionContext) -> None:
    context.log.info("Starting MRF matching...")
    run_mrf_matcher()
    context.log.info("MRF matching complete.")


@dg.asset(
    group_name="ingestion",
    deps=[mrf_matcher],
    description="Streams raw MRF files from hospital URLs to S3 bronze layer.",
)
def mrf_ingest(context: dg.AssetExecutionContext) -> None:
    context.log.info("Starting MRF ingestion...")
    run_mrf_ingest()
    context.log.info("MRF ingestion complete.")


@dg.asset(
    key=AssetKey(["bronze", "mrf_raw"]),
    group_name="transform",
    deps=[mrf_ingest],
    description="Transforms raw MRF files from S3 bronze volume to Silver Delta table via Databricks.",
)
def mrf_transform(context: dg.AssetExecutionContext) -> None:
    context.log.info("Starting MRF transform...")
    run_mrf_transform()
    context.log.info("MRF transform complete.")


@dg.asset(
    key=AssetKey(["bronze", "cms_quality"]),
    group_name="ingestion",
    description="Fetches CMS Care Compare quality scores for Nashville MSA hospitals and uploads to S3 bronze layer.",
)
def fetch_quality_scores(context: dg.AssetExecutionContext) -> None:
    context.log.info("Starting quality scores fetch...")
    run_fetch_quality_scores()
    context.log.info("Quality scores fetch complete.")


# ── dbt Assets ────────────────────────────────────────────────────────────────

@dbt_assets(
    manifest=DBT_MANIFEST,
    select="silver",
    dagster_dbt_translator=SilverDbtTranslator(),
)
def dbt_silver_assets(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["run", "--select", "silver"], context=context).stream()


@dbt_assets(
    manifest=DBT_MANIFEST,
    select="gold",
    dagster_dbt_translator=GoldDbtTranslator(),
)
def dbt_gold_assets(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["run", "--select", "gold"], context=context).stream()