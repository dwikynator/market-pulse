import os
import subprocess
from datetime import date, timedelta

import pendulum
from airflow.sdk import Param, dag, get_current_context, task

# Absolute paths inside the container — avoids working-directory ambiguity.
PROJECT_ROOT = "/opt/project"
# Use the project venv, not Airflow's Python, to keep dependency sets separate.
PROJECT_PYTHON = "/opt/project/.venv/bin/python"


def run_project_command(arguments: list[str]) -> None:
    # check=True converts a non-zero exit into an exception so Airflow retries.
    # stdout/stderr are not captured, so output goes straight to the task log.
    subprocess.run(
        [PROJECT_PYTHON, *arguments],
        cwd=PROJECT_ROOT,
        check=True,
    )


@dag(
    dag_id="market_pulse_daily",
    description="Collect bounded market and economic data and prepare it for loading.",
    schedule="0 23 * * 1-5",        # 23:00 UTC Mon–Fri; markets closed, final data available
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,                  # historical runs must be triggered explicitly via backfill
    max_active_runs=1,              # one date at a time — keeps provider calls modest
    params={
        "as_of": Param(
            "",
            type="string",
            pattern=r"^$|^\d{4}-\d{2}-\d{2}$",   # empty = use logical_date; else YYYY-MM-DD
            description="Optional YYYY-MM-DD date. Leave empty for the run's logical date.",
        )
    },
    tags=["market-pulse", "batch"],
)
def market_pulse_daily():
    @task
    def resolve_as_of() -> str:
        # Manual trigger: use the supplied date.
        # Scheduled / backfill: derive from logical_date so each run gets its own date.
        context = get_current_context()
        requested = context["params"]["as_of"]
        if requested:
            return date.fromisoformat(requested).isoformat()
        return context["logical_date"].date().isoformat()

    @task(
        retries=2,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=30),  # safety ceiling; normal run is a few minutes
    )
    def collect_batch(as_of: str) -> str:
        # Runs the same entry point used in manual development (python -m market_pulse.batch).
        # The batch writes stable, date-partitioned S3 keys — retrying the same date is safe.
        bucket = os.environ.get("DATA_BUCKET")
        if not bucket:
            raise RuntimeError("DATA_BUCKET is not configured in the Airflow container")

        run_project_command(
            [
                "-m",
                "market_pulse.batch",
                "--bucket",
                bucket,
                "--as-of",
                as_of,
            ]
        )
        return as_of

    @task(
        retries=2,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=10),
    )
    def inspect_batch_summary(as_of: str) -> dict[str, str]:
        # Reads audit/as_of=YYYY-MM-DD/summary.json — written last by collect_batch,
        # so its presence confirms all four data objects landed in S3 successfully.
        bucket = os.environ.get("DATA_BUCKET")
        if not bucket:
            raise RuntimeError("DATA_BUCKET is not configured in the Airflow container")

        run_project_command(
            [
                "scripts/inspect_batch_summary.py",
                "--bucket",
                bucket,
                "--as-of",
                as_of,
            ]
        )
        return {
            "as_of": as_of,
            "audit_uri": f"s3://{bucket}/audit/as_of={as_of}/summary.json",
        }

    @task
    def ready_for_warehouse(summary: dict[str, str]) -> None:
        # Explicit handoff marker for Part 7 (Snowflake + dbt).
        print(f"Ready for loading: {summary['as_of']}")
        print(f"Completion marker: {summary['audit_uri']}")

    # TaskFlow infers dependencies from argument passing:
    # resolve_as_of → collect_batch → inspect_batch_summary → ready_for_warehouse
    as_of = resolve_as_of()
    collected_as_of = collect_batch(as_of)
    summary = inspect_batch_summary(collected_as_of)
    ready_for_warehouse(summary)


market_pulse_daily()
