"""Silver writer-lease behaviour, run in-process with Spark stubbed out.

Runs inside the dagster-code container (scripts/verify.sh pipes it in); prints OK or fails.
The race it covers: the nightly schedule plans a full run while the CDC stream's lease is
stale, then the stream renews its lease before the silver step starts.
"""

import dagster as dg

import lakehouse_orchestration.definitions as d

PIPES = {"pipes": dg.PipesSubprocessClient()}


def steps(result):
    return {
        e.step_key: e.event_type_value
        for e in result.all_events
        if e.event_type_value in ("STEP_SUCCESS", "STEP_FAILURE", "STEP_SKIPPED")
    }


def events(result, kind):
    return [e for e in result.all_events if e.event_type_value == kind]


def no_spark(*_args, **_kwargs):
    raise AssertionError("Spark must not start while the stream owns silver")


class FakeSpark:
    """Stands in for the Pipes session: publishes every silver table with its WAP checks."""

    def __init__(self, context):
        self.context = context

    def get_results(self, implicit_materializations=False):
        for key in sorted(d.SILVER_KEYS):
            yield dg.MaterializeResult(asset_key=key)
        for ck in self.context.selected_asset_check_keys:
            yield dg.AssetCheckResult(
                asset_key=ck.asset_key, check_name=ck.name, passed=True
            )


# 1. Scheduled run, stream took the lease after planning: skip silver, skip gold, succeed.
d.silver_lease_holder = lambda: "corebank-cdc-v1"
d.spark_step = no_spark
r = dg.materialize(
    [d.silver, d.gold_customer_360],
    resources=PIPES,
    tags={"dagster/schedule_name": "nightly_refresh"},
    raise_on_error=False,
)
assert r.success, "scheduled run must not fail when the stream owns silver"
assert steps(r) == {"silver": "STEP_SUCCESS", "gold_customer_360": "STEP_SKIPPED"}, (
    steps(r)
)
assert not events(r, "ASSET_MATERIALIZATION") and not events(
    r, "ASSET_CHECK_EVALUATION"
)

# 2. Manual backfill while the stream owns silver: refuse loudly.
r = dg.materialize([d.silver], resources=PIPES, raise_on_error=False)
failures = events(r, "STEP_FAILURE")
assert not r.success and "live lease" in failures[0].event_specific_data.error.message

# 3. No live lease: the whole unit publishes, with every WAP check result.
d.silver_lease_holder = lambda: None
d.spark_step = lambda context, pipes, step: FakeSpark(context)
r = dg.materialize([d.silver], resources=PIPES, raise_on_error=False)
assert r.success
assert {e.asset_key for e in events(r, "ASSET_MATERIALIZATION")} == d.SILVER_KEYS
assert len(events(r, "ASSET_CHECK_EVALUATION")) == len(
    d.silver.check_specs_by_output_name
)

# 4. A partial selection is refused: silver is built as one unit.
r = dg.materialize(
    [d.silver],
    selection=[sorted(d.SILVER_KEYS)[0]],
    resources=PIPES,
    raise_on_error=False,
)
assert not r.success

print("OK")
