# Dagster control plane: webserver (UI + GraphQL) and daemon (schedules, sensors,
# run queue, run monitoring). No pipeline code here: runs execute in the code server.
FROM python:3.12-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:0.9 /uv /usr/local/bin/uv
COPY requirements.txt /tmp/requirements.txt
# uid 185 matches the code server (Spark image), so both can share the step-log volume.
RUN uv pip install --system --require-hashes --no-cache -r /tmp/requirements.txt \
 && useradd --system --uid 185 --home /opt/dagster dagster \
 && mkdir -p /opt/dagster/home /opt/dagster/logs && chown -R dagster /opt/dagster
COPY workspace.yaml /opt/dagster/home/workspace.yaml
ENV DAGSTER_HOME=/opt/dagster/home
USER 185
WORKDIR /opt/dagster/home
