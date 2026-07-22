from __future__ import annotations

import pendulum

from airflow.sdk import dag, task


@dag(
    dag_id="hello_lab",
    # When the DAG becomes eligible to run. Must be in the past.
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    # Cron, timedelta, or a preset. None = manual trigger only.
    schedule="@daily",
    # False = don't backfill every missed interval since start_date.
    # Leave this False unless you deliberately want backfilling.
    catchup=False,
    # Shows up in the UI's tag filter (the "Фильтр по тегу" dropdown).
    tags=["lab", "smoke-test"],
)
def hello_lab():
    @task
    def extract() -> dict:
        # Pretend this hit an API. The return value is pushed to XCom.
        return {"rows": 42, "source": "fake_api"}

    @task
    def report(payload: dict) -> None:
        # Pulled from XCom automatically because we passed it as an argument.
        print(f"Got {payload['rows']} rows from {payload['source']}")

    # Calling the task functions builds the dependency graph.
    # extract() runs first because report() needs its output.
    report(extract())


# Critical: the decorated function must be CALLED at module level.
# If you forget these parens, the DAG never registers and you get
# no error message — just an empty DAG list.
hello_lab()