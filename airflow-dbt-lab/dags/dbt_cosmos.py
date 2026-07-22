from __future__ import annotations

import pendulum
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig

# Переиспользуем наш готовый profiles.yml. Cosmos-LOCAL крутится внутри
# Airflow-контейнера, а он в той же Docker-сети — значит host `warehouse`
# из profiles.yml резолвится нормально.
profile_config = ProfileConfig(
    profile_name="lab",
    target_name="dev",
    profiles_yml_filepath="/opt/airflow/dbt/profiles.yml",
)

# dbt, поставленный через pip под пользователем airflow, лежит здесь.
execution_config = ExecutionConfig(
    dbt_executable_path="/home/airflow/.local/bin/dbt",
)

# DbtDag сам строит по одной задаче на каждую модель и тест.
# Никаких BashOperator вручную — Cosmos парсит проект и разворачивает граф.
dbt_cosmos = DbtDag(
    project_config=ProjectConfig("/opt/airflow/dbt"),
    profile_config=profile_config,
    execution_config=execution_config,
    dag_id="dbt_cosmos",
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["lab", "cosmos"],
)