"""What a tenant can do to its own namespace without asking the platform, proven as the canary.

Create a table and a view, rename the table (a Kappa replay swap is two renames), drop both: each
statement needs a Polaris privilege the tenant writer role must hold, and Spark's DROP purges, which
the catalog must allow. Everything is scratch and gone at the end, so it is safe to re-run.
"""

from __future__ import annotations

import os

from common import spark_session
from dagster_pipes import open_dagster_pipes

NS = "canary_data"
A, B, V = f"{NS}.scratch_a", f"{NS}.scratch_b", f"{NS}.scratch_v"


def main() -> None:
    spark = spark_session("canary-self-service")
    for name in (V,):
        spark.sql(f"DROP VIEW IF EXISTS {name}")
    for name in (A, B):
        spark.sql(f"DROP TABLE IF EXISTS {name}")
    spark.sql(f"CREATE TABLE {A} (id int) USING iceberg")
    spark.sql(f"INSERT INTO {A} VALUES (1), (2)")
    spark.sql(f"CREATE VIEW {V} AS SELECT * FROM {A}")
    assert spark.table(V).count() == 2, "a view over the tenant's table reads back"
    spark.sql(f"DROP VIEW {V}")
    spark.sql(f"ALTER TABLE {A} RENAME TO {B}")
    assert spark.table(B).count() == 2, "a renamed table keeps its rows"
    spark.sql(f"DROP TABLE {B}")
    left = [r.tableName for r in spark.sql(f"SHOW TABLES IN {NS}").collect() if r.tableName.startswith("scratch_")]
    assert not left, f"scratch objects left behind: {left}"
    print("[canary] self-service: create table and view, rename, drop: ok")
    if "DAGSTER_PIPES_CONTEXT" in os.environ:
        with open_dagster_pipes() as pipes:
            pipes.report_asset_materialization(
                metadata={"operations": "create table, create view, rename, drop view, drop table"}
            )


if __name__ == "__main__":
    main()
