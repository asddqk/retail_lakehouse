# Образ Airflow с зависимостями проекта.
# Версии зафиксированы: с ними проект проверен (см. .github/workflows/ci.yml).
FROM apache/airflow:3.1.8-python3.12

# dbt ставится в отдельное окружение (рекомендация Cosmos):
# его зависимости не конфликтуют с зависимостями Airflow.
RUN python -m venv /opt/airflow/dbt_venv \
 && /opt/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-core==1.12.6 dbt-trino==1.11.0

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt

# Каталог для базы Airflow (SQLite) — монтируется как том, чтобы история запусков не терялась
RUN mkdir -p /opt/airflow/state

ENV PYTHONPATH=/opt/airflow/src
