"""Grounding guardrail: every fact in a guidance card must come from evidence.

Before a card reaches the colleague, each *checkable fact* in its text (money
amounts, dates, customer/account/transaction/complaint ids) must appear in the tool
results or the transcript that justify it. A card that states a fact we can't
trace is withheld and counted (metric + eval). A wrong number read to a customer is
worse than no number. This applies to template text and LLM text alike.
"""

from __future__ import annotations

import json
import re
from decimal import Decimal, InvalidOperation
from typing import Any

MONEY = re.compile(r"£\s?(\d{1,3}(?:,\d{3})*(?:\.\d{2})?|\d+(?:\.\d{2})?)")
DATE = re.compile(r"\b(\d{4}-\d{2}-\d{2})\b")
IDS = re.compile(r"\b(C\d{7}|A\d{8}|CMP\d{7}|T[0-9A-Za-z]{10,24})\b")


def _numbers(blob: str) -> set[Decimal]:
    out = set()
    for m in re.findall(r"-?\d+(?:\.\d+)?", blob):
        try:
            out.add(abs(Decimal(m)).quantize(Decimal("0.01")))
        except InvalidOperation:
            continue
    return out


def facts(text: str) -> dict[str, set[str]]:
    return {
        "money": {m.replace(",", "") for m in MONEY.findall(text)},
        "dates": set(DATE.findall(text)),
        "ids": set(IDS.findall(text)),
    }


def ungrounded(text: str, evidence: list[Any], transcript: str = "") -> list[str]:
    """The facts in `text` that no evidence supports (empty list = grounded)."""
    blob = json.dumps(evidence, default=str) + " " + transcript
    numbers = _numbers(blob)
    missing = []
    f = facts(text)
    for m in f["money"]:
        if Decimal(m).quantize(Decimal("0.01")) not in numbers:
            missing.append(f"£{m}")
    missing += [d for d in f["dates"] if d not in blob]
    missing += [i for i in f["ids"] if i not in blob]
    return missing
