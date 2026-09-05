FROM apache/airflow:2.10.4-python3.12

# Dependências do pipeline (extract/load em Python + transformação em dbt).
# Ficam na imagem, não no requirements.txt do Airflow em si, para não
# conflitar com as constraints oficiais do projeto Airflow.
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
