"""
load.py
-------
Camada de carga (L do ELT): lê todos os arquivos JSON brutos em
data/raw/ e carrega em uma tabela `raw.air_quality_measurements`
dentro de um arquivo DuckDB local.

Decisão de arquitetura: DuckDB foi escolhido para este projeto por ser
um banco analítico embarcado (zero infraestrutura para rodar/testar),
mas o restante do pipeline (dbt) é portável para Postgres/BigQuery/
Snowflake trocando apenas o profile do dbt.

Uso:
    python src/load.py
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import duckdb
import pandas as pd

RAW_DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
DB_PATH = Path(__file__).resolve().parent.parent / "data" / "warehouse.duckdb"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("load")


def flatten_record(record: dict, extracted_at: str) -> dict:
    return {
        "city_query": record.get("_city_query"),
        "location_id": record.get("_location_id"),
        "location_name": record.get("_location_name"),
        "parameter": record.get("parameter"),
        "value": record.get("value"),
        "unit": record.get("unit"),
        "measured_at_utc": record.get("date", {}).get("utc"),
        "measured_at_local": record.get("date", {}).get("local"),
        "latitude": record.get("coordinates", {}).get("latitude"),
        "longitude": record.get("coordinates", {}).get("longitude"),
        "extracted_at": extracted_at,
    }


def load_raw_files() -> pd.DataFrame:
    files = sorted(RAW_DATA_DIR.glob("openaq_*.json"))
    if not files:
        raise FileNotFoundError(
            f"Nenhum arquivo openaq_*.json encontrado em {RAW_DATA_DIR}. "
            "Rode src/extract.py (dados reais) ou src/generate_sample_data.py (dados sintéticos) primeiro."
        )

    rows = []
    for f in files:
        payload = json.loads(f.read_text(encoding="utf-8"))
        extracted_at = payload.get("extracted_at")
        for record in payload.get("results", []):
            rows.append(flatten_record(record, extracted_at))
        log.info("Lido %s (%d registros)", f.name, len(payload.get("results", [])))

    return pd.DataFrame(rows)


def load_to_duckdb(df: pd.DataFrame) -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    con.execute("CREATE OR REPLACE TABLE raw.air_quality_measurements AS SELECT * FROM df;")
    count = con.execute("SELECT COUNT(*) FROM raw.air_quality_measurements;").fetchone()[0]
    con.close()
    log.info("Carregados %d registros em raw.air_quality_measurements (%s)", count, DB_PATH)


if __name__ == "__main__":
    df = load_raw_files()
    load_to_duckdb(df)
