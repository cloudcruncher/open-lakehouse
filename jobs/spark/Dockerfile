# Spark 3.5 runtime with every jar baked in at build time, so jobs never download
# dependencies at run time (reproducible, works air-gapped, and a supply-chain change
# is a reviewed image rebuild rather than a surprise). Each jar is checked against the
# SHA-1 Maven Central publishes; Python dependencies are installed from hashes.
FROM apache/spark:3.5.9-java17-python3

ARG ICEBERG_VERSION=1.11.0
ARG PG_JDBC_VERSION=42.7.13
ARG SPARK_VERSION=3.5.9
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1
ARG OPENLINEAGE_VERSION=1.53.0
ARG MAVEN=https://repo1.maven.org/maven2

USER root
RUN set -eux; cd /opt/spark/jars; \
    for jar in \
      "org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
      "org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
      "org/postgresql/postgresql/${PG_JDBC_VERSION}/postgresql-${PG_JDBC_VERSION}.jar" \
      "org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar" \
      "org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar" \
      "org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" \
      "org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar" \
      "io/openlineage/openlineage-spark_2.12/${OPENLINEAGE_VERSION}/openlineage-spark_2.12-${OPENLINEAGE_VERSION}.jar"; do \
      f="$(basename "$jar")"; \
      curl -fsSLo "$f" "${MAVEN}/${jar}"; \
      echo "$(curl -fsSL "${MAVEN}/${jar}.sha1" | cut -c1-40)  $f" | sha1sum -c -; \
    done

COPY --from=ghcr.io/astral-sh/uv:0.9 /uv /usr/local/bin/uv
COPY requirements.txt /tmp/requirements.txt
RUN uv pip install --system --break-system-packages --require-hashes --no-cache -r /tmp/requirements.txt

# Streaming checkpoints live on a volume; pre-create it so the volume is owned by spark.
RUN mkdir -p /checkpoints /opt/dagster/home /opt/dagster/logs && chown spark:spark /checkpoints /opt/dagster/home /opt/dagster/logs
COPY pipelines /opt/pipelines
COPY orchestration /opt/orchestration
ENV PYTHONPATH=/opt/orchestration
USER spark
WORKDIR /opt/pipelines
ENTRYPOINT ["/opt/pipelines/run.sh"]
