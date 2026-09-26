# Superset release images ship without drivers. Add the metadata-store driver, the Trino
# driver for step B, and Authlib for Keycloak sign-in.
FROM apache/superset:6.1.0

USER root
RUN . /app/.venv/bin/activate && uv pip install --no-cache \
    psycopg2-binary==2.9.13 \
    "trino[sqlalchemy]==0.340.0" \
    Authlib==1.8.0
USER superset

COPY superset_config.py provision.py /app/pythonpath/
