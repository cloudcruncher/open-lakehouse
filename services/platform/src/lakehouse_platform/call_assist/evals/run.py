"""Offline evals for Live Call Assist: understanding quality and end-to-end call behaviour.

Two suites, no platform needed (CI runs them on every change):

  understanding  labelled caller utterances -> per-label precision / recall of the
                 extractor, plus name/postcode capture. Reported honestly: the rules
                 extractor misses some paraphrases on purpose.
  calls          scripted calls against recorded tool responses -> tool selection,
                 expected cards, grounding (zero withheld), content that must never
                 reach the colleague, and graceful failure handling.

Gates (exit code 1 on breach): every call case passes, zero ungrounded cards,
risk-signal recall = 100% (safety labels must never be missed), overall intent
and vulnerability recall at or above the floor, and precision at or above its floor
for every label group. The floors ratchet up as the extractor improves; they never
silently go down. With --extractor claude the report also says how the model was used:
answered vs fell back to rules (and why), latency, tokens, prompt-cache use and cost.

Usage: call-assist-evals [--extractor rules|claude] [--json report.json]
"""

from __future__ import annotations

import argparse
import asyncio
import copy
import json
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any

import yaml

from ..engine import CallSession, Utterance
from ..knowledge import ProcedureIndex
from ..signals import ClaudeExtractor, RulesExtractor
from ..tools import ToolFailure

HERE = Path(__file__).parent
SINGULAR = {"intents": "intent", "vulnerabilities": "vulnerability", "risks": "risk"}
RECALL_FLOOR = {"intents": 0.80, "vulnerabilities": 0.75, "risks": 1.0}
# A false label is not harmless: a false intent fetches data and shows a card nobody asked for.
PRECISION_FLOOR = {"intents": 0.90, "vulnerabilities": 0.90, "risks": 0.90}
# USD per million tokens (list price) for the cost estimate: input, output. Cache reads
# are billed at 0.1x input and 5-minute cache writes at 1.25x.
PRICES = {"claude-haiku-4-5": (1.0, 5.0)}


class FixtureTools:
    """Replays recorded gateway responses; a list replays in order (last one repeats)."""

    def __init__(self, fixtures: dict[str, Any]) -> None:
        self.fixtures = copy.deepcopy(fixtures)
        self.calls: list[str] = []

    async def call(self, tool: str, args: dict[str, Any]) -> dict[str, Any]:
        self.calls.append(tool)
        if tool not in self.fixtures:
            raise ToolFailure("error", f"no fixture for {tool}")
        fx = self.fixtures[tool]
        if isinstance(fx, list):
            fx = fx.pop(0) if len(fx) > 1 else fx[0]
        if "error" in fx:
            raise ToolFailure(fx["error"], f"Customer data {fx['error']}")
        return copy.deepcopy(fx)

    async def close(self) -> None:
        return None


# ------------------------------------------------------------ understanding
def eval_understanding(extractor: Any) -> dict[str, Any]:
    items = yaml.safe_load((HERE / "utterances.yaml").read_text())
    tp: dict[str, int] = defaultdict(int)
    fp: dict[str, int] = defaultdict(int)
    fn: dict[str, int] = defaultdict(int)
    misses: list[str] = []
    ident_ok = ident_total = 0
    traces: list[dict[str, Any]] = []
    for item in items:
        got, trace = extractor.trace(item["text"], "customer")
        traces.append(trace)
        for field in ("intents", "vulnerabilities", "risks"):
            want = set(item.get(field) or [])
            have = {str(x) for x in getattr(got, field)}
            tp[field] += len(have & want)
            for label in have - want:
                fp[field] += 1
                misses.append(f"false {SINGULAR[field]} {label!r}: {item['text']}")
            for label in want - have:
                fn[field] += 1
                misses.append(f"missed {SINGULAR[field]} {label!r}: {item['text']}")
        if "name" in item:
            ident_total += 1
            ident_ok += int(got.full_name == item["name"] and got.postcode == item["postcode"])
    report = {}
    for field in ("intents", "vulnerabilities", "risks"):
        p = tp[field] / max(1, tp[field] + fp[field])
        r = tp[field] / max(1, tp[field] + fn[field])
        report[field] = {
            "precision": round(p, 3),
            "recall": round(r, 3),
            "tp": tp[field],
            "fp": fp[field],
            "fn": fn[field],
        }
    report["identity_capture"] = round(ident_ok / max(1, ident_total), 3)
    report["misses"] = misses
    report["model"] = model_usage(traces)
    return report


def model_usage(traces: list[dict[str, Any]]) -> dict[str, Any] | None:
    """How the model was really used: answered vs fell back (and why), latency, tokens."""
    asked = [t for t in traces if "model" in t]
    if not asked:
        return None
    ok = [t for t in asked if t["engine"] == "claude"]
    ms = sorted(t["ms"] for t in ok)
    return {
        "model": next((t["model"] for t in ok), asked[0]["model"]),
        "calls": len(asked),
        "answered": len(ok),
        "fallbacks": sorted(t["fallback"] for t in asked if "fallback" in t),
        "p50_ms": ms[len(ms) // 2] if ms else None,
        "p95_ms": ms[min(len(ms) - 1, int(len(ms) * 0.95))] if ms else None,
        "input_tokens": sum(t["input_tokens"] for t in ok),
        "output_tokens": sum(t["output_tokens"] for t in ok),
        "cache_read_tokens": sum(t.get("cache_read_tokens", 0) for t in ok),
        "cache_write_tokens": sum(t.get("cache_write_tokens", 0) for t in ok),
        "cost_usd": cost_usd(ok),
    }


def cost_usd(traces: list[dict[str, Any]]) -> float | None:
    model = next((t["model"] for t in traces), "")
    price = next((p for name, p in PRICES.items() if model.startswith(name)), None)
    if price is None:
        return None
    per_in, per_out = price[0] / 1e6, price[1] / 1e6
    total = sum(
        t["input_tokens"] * per_in
        + t.get("cache_read_tokens", 0) * per_in * 0.1
        + t.get("cache_write_tokens", 0) * per_in * 1.25
        + t["output_tokens"] * per_out
        for t in traces
    )
    return round(total, 4)


# -------------------------------------------------------------------- calls
async def run_call(
    case: dict[str, Any], today: date, extractor: Any, sink: list[dict[str, Any]] | None = None
) -> dict[str, Any]:
    events: list[dict[str, Any]] = [] if sink is None else sink

    async def emit(ev: dict[str, Any]) -> None:
        events.append(ev)

    tools = FixtureTools(case.get("tools", {}))
    session = CallSession(
        f"EVAL-{case['name'][:40]}",
        "alice",
        emit,
        extractor,
        ProcedureIndex.default(),
        tools,
        today=lambda: today,
        fresh_retry_delay_s=0,
    )
    for seq, (speaker, text) in enumerate(case["lines"], start=1):
        await session.on_utterance(Utterance(session.call_id, "alice", seq, speaker, text))
    cards = [e["card"] for e in events if e["type"] == "card"]
    withheld = [e for e in events if e["type"] == "withheld"]
    exp = case.get("expect", {})
    failures = []
    if "tools" in exp and sorted(set(tools.calls)) != sorted(set(exp["tools"])):
        failures.append(f"tools {sorted(set(tools.calls))} != {sorted(set(exp['tools']))}")
    titles = [c["title"] for c in cards]
    for want in exp.get("cards", []):
        if not any(t.startswith(want) for t in titles):
            failures.append(f"missing card {want!r} (got {titles})")
    kinds = {c["kind"] for c in cards}
    for k in exp.get("kinds", []):
        if k not in kinds:
            failures.append(f"missing card kind {k}")
    text = " ".join(f"{c['title']} {c['body']}" for c in cards)
    for s in exp.get("body_contains", []):
        if s not in text:
            failures.append(f"expected text {s!r} not shown")
    for s in exp.get("must_not_contain", []):
        if s in text:
            failures.append(f"forbidden text {s!r} shown")
    if withheld:
        failures.append(f"{len(withheld)} ungrounded card(s) withheld: {[w['title'] for w in withheld]}")
    return {
        "name": case["name"],
        "passed": not failures,
        "failures": failures,
        "cards": titles,
        "tools": tools.calls,
        "withheld": len(withheld),
    }


def eval_calls(extractor: Any) -> list[dict[str, Any]]:
    spec = yaml.safe_load((HERE / "calls.yaml").read_text())
    today = date.fromisoformat(spec["today"])
    return [asyncio.run(run_call(case, today, extractor)) for case in spec["cases"]]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--extractor", choices=["rules", "claude"], default="rules")
    ap.add_argument("--json", type=Path)
    args = ap.parse_args()
    extractor = ClaudeExtractor() if args.extractor == "claude" else RulesExtractor()

    u = eval_understanding(extractor)
    calls = eval_calls(extractor)

    print(f"Live Call Assist evals · extractor={extractor.name}\n")
    print("Understanding (labelled utterances)")
    print(f"  {'label':<16}{'precision':>10}{'recall':>9}{'floors (p/r)':>14}")
    gates = []
    for field in ("intents", "vulnerabilities", "risks"):
        m = u[field]
        p_floor, r_floor = PRECISION_FLOOR[field], RECALL_FLOOR[field]
        ok = m["precision"] >= p_floor and m["recall"] >= r_floor
        gates.append(ok)
        print(
            f"  {field:<16}{m['precision']:>10.2f}{m['recall']:>9.2f}{p_floor:>9.2f}/{r_floor:.2f}"
            f"  {'ok' if ok else 'BELOW FLOOR'}"
        )
    print(f"  identity capture (name + postcode): {u['identity_capture']:.0%}")
    if m := u["model"]:
        print(
            f"  model {m['model']}: answered {m['answered']}/{m['calls']}, "
            f"p50 {m['p50_ms']} ms, p95 {m['p95_ms']} ms, "
            f"tokens {m['input_tokens']} in / {m['output_tokens']} out"
        )
        cost = f"≈ ${m['cost_usd']:.4f} at list price" if m["cost_usd"] is not None else "cost unknown"
        print(f"  prompt cache: {m['cache_read_tokens']} read, {m['cache_write_tokens']} written · {cost}")
        if m["fallbacks"]:
            print(f"  fell back to rules: {', '.join(m['fallbacks'])}")
    if u["misses"]:
        print("  misses:")
        for miss in u["misses"]:
            print(f"    - {miss}")

    print("\nCalls (recorded tool responses)")
    for c in calls:
        print(f"  {'PASS' if c['passed'] else 'FAIL'}  {c['name']}")
        for f in c["failures"]:
            print(f"        {f}")
    withheld = sum(c["withheld"] for c in calls)
    passed = sum(c["passed"] for c in calls)
    print(f"\n  {passed}/{len(calls)} call cases passed · ungrounded cards shown: 0 · withheld: {withheld}")
    gates.append(passed == len(calls))

    if args.json:
        args.json.write_text(json.dumps({"understanding": u, "calls": calls}, indent=2))
    ok = all(gates)
    print(f"\nGate: {'PASS' if ok else 'FAIL'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
