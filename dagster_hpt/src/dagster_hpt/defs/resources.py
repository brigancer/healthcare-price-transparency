from pathlib import Path

import dagster as dg
from dagster_dbt import DbtCliResource

DBT_PROJECT_DIR = Path(__file__).parent.parent.parent.parent.parent / "dbt_hpt_pipeline"


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "dbt": DbtCliResource(
                project_dir=str(DBT_PROJECT_DIR),
                dbt_executable=r"C:\Users\briga\.local\bin\dbt.exe",
            )
        }
    )