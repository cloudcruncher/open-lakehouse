# Airflow 3 on the Spark pipeline image: tasks run /opt/pipelines/run.sh in-process (LocalExecutor),
# the same way Dagster's code server does. Airflow lives in its own venv so its pinned dependency
# set (the project's constraints file for this exact version) never disturbs the Spark image's hashed
# Python packages. Built with Compose, which supplies the `spark` context (the dagster-code image).
FROM spark
ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.10
USER root
# The VCS client the DAG-bundle provider shells out to (tenants deliver DAGs from their own repos).
RUN apt-get update && apt-get install -y --no-install-recommends git \
 && rm -rf /var/lib/apt/lists/* \
 && uv venv --python /usr/bin/python3 /opt/airflow-venv \
 && uv pip install --python /opt/airflow-venv/bin/python --no-cache \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
      "apache-airflow[postgres]==${AIRFLOW_VERSION}" apache-airflow-providers-openlineage apache-airflow-providers-git \
 && mkdir -p /opt/airflow/logs && chown -R spark:spark /opt/airflow
ENV PATH=/opt/airflow-venv/bin:$PATH \
    AIRFLOW_HOME=/opt/airflow \
    AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags \
    PYTHONPATH=/opt/airflow/dags
USER spark
WORKDIR /opt/airflow
ENTRYPOINT ["airflow"]
