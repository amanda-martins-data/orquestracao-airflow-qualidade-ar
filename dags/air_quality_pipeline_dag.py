"""
air_quality_pipeline_dag.py
----------------------------
Orquestra o pipeline de qualidade do ar construído no Projeto 01
(extract -> load -> dbt build) com Airflow.

Decisões de design (ver docs/architecture.md para detalhes):
- TaskFlow API (@dag/@task) para as etapas em Python; BashOperator para o dbt.
- Um branch decide, via Airflow Variable, entre extração real (OpenAQ,
  precisa de API key) ou dados sintéticos (fixture, sem dependências
  externas) — assim a DAG roda "out of the box" em qualquer ambiente.
- Retries com backoff exponencial: falhas de rede na API são transitórias.
- schedule="@daily" com catchup=False: cada execução processa o dia
  corrente, sem backfill automático de histórico.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator

INCLUDE_DIR = Path(__file__).resolve().parent.parent / "include"
SRC_DIR = INCLUDE_DIR / "src"
DBT_PROJECT_DIR = INCLUDE_DIR / "dbt_project"

sys.path.insert(0, str(SRC_DIR))

default_args = {
    "owner": "amanda.martins",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}


def _choose_extraction_source(**context) -> str:
    """Decide a fonte de extração via Airflow Variable, sem precisar editar a DAG."""
    use_synthetic = Variable.get("air_quality_use_synthetic", default_var="true").lower() == "true"
    return "generate_sample_data" if use_synthetic else "extract_from_openaq"


@dag(
    dag_id="air_quality_pipeline",
    description="Pipeline de qualidade do ar: extract -> load -> dbt build",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["portfolio", "elt", "air-quality", "projeto-02"],
)
def air_quality_pipeline():
    choose_source = BranchPythonOperator(
        task_id="choose_extraction_source",
        python_callable=_choose_extraction_source,
    )

    @task(task_id="extract_from_openaq")
    def extract_from_openaq() -> None:
        from extract import extract_for_cities

        cities_raw = Variable.get(
            "air_quality_cities", default_var="São Paulo,Rio de Janeiro,Belo Horizonte"
        )
        cities = [c.strip() for c in cities_raw.split(",") if c.strip()]
        extract_for_cities(cities, days_back=1)

    @task(task_id="generate_sample_data")
    def generate_sample_data() -> None:
        from generate_sample_data import generate

        generate(days_back=1)

    @task(task_id="load_to_duckdb", trigger_rule="none_failed_min_one_success")
    def load_to_duckdb() -> None:
        from load import load_raw_files
        from load import load_to_duckdb as _load_to_duckdb

        df = load_raw_files()
        _load_to_duckdb(df)

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"cp -n {DBT_PROJECT_DIR}/profiles.yml.example {DBT_PROJECT_DIR}/profiles.yml "
            f"2>/dev/null; "
            f"cd {DBT_PROJECT_DIR} && dbt build --profiles-dir {DBT_PROJECT_DIR}"
        ),
    )

    extract_task = extract_from_openaq()
    sample_task = generate_sample_data()
    load_task = load_to_duckdb()

    choose_source >> [extract_task, sample_task] >> load_task >> dbt_build


air_quality_pipeline()
