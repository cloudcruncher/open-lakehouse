"""Write the canary's five synthetic people as the canary's own Polaris principal (ADR 14).

Idempotent: the table is replaced each run, so re-running never duplicates rows. The email
column is tagged pii.contact in contracts/canary.odcs.yaml; the platform checks that it is
masked for colleagues without PII clearance.
"""

from __future__ import annotations

import os

from common import spark_session  # the platform's session: vended credentials, no storage keys
from dagster_pipes import open_dagster_pipes

TABLE = "canary_data.people"
PEOPLE = [
    (1, "Ada Canary", "ada@canary.example", "north"),
    (2, "Ben Canary", "ben@canary.example", "north"),
    (3, "Cy Canary", "cy@canary.example", "south"),
    (4, "Di Canary", "di@canary.example", "south"),
    (5, "Ed Canary", "ed@canary.example", "west"),
]


def main() -> None:
    spark = spark_session("canary-seed")
    df = spark.createDataFrame(PEOPLE, "person_id int, name string, email string, region string")
    df.writeTo(TABLE).using("iceberg").createOrReplace()
    rows = spark.table(TABLE).count()
    snapshot = spark.sql(f"SELECT snapshot_id FROM {TABLE}.snapshots ORDER BY committed_at DESC LIMIT 1").first()[0]
    print(f"[canary] {TABLE}: {rows} rows, snapshot {snapshot}")
    if "DAGSTER_PIPES_CONTEXT" in os.environ:
        with open_dagster_pipes() as pipes:
            pipes.report_asset_materialization(
                metadata={"rows": rows, "snapshot_id": str(snapshot)}
            )


if __name__ == "__main__":
    main()
