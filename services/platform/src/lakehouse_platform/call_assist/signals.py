"""Understanding the call: turn transcript text into structured signals.

Two interchangeable extractors produce the same `Signals` schema:

  * RulesExtractor   deterministic patterns. Always available, zero latency, fully
                     testable, the fallback when the model is slow or down.
  * ClaudeExtractor  an LLM with a forced tool call (structured output), for the
                     long tail of phrasing that patterns miss. Used only when
                     ANTHROPIC_API_KEY is set. Its output is validated against the
                     same schema and merged with the rules output, so the model can
                     add recall but can't remove what the rules found.

Both expose `trace(text, speaker) -> (Signals, trace)`. The trace says which engine
understood the utterance and, for Claude, the model, request id, latency, tokens, and
what it added beyond the rules; when Claude wasn't used it says why. The console shows
it under each caller line, so "is the model actually answering?" is visible per turn.

Neither extractor calls tools or sees customer data. The planner (engine.py) decides
what to fetch, under fixed policy: the model proposes, policy disposes.
"""

from __future__ import annotations

import logging
import os
import re
import time
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Any

from prometheus_client import Counter, Histogram
from pydantic import BaseModel, Field

from .budget import BUDGET, SpendBudget

log = logging.getLogger(__name__)

LLM_CALLS = Counter(
    "assist_llm_extractions_total", "Claude extraction calls by outcome", ["model", "outcome"]
)
LLM_LATENCY = Histogram(
    "assist_llm_extraction_seconds",
    "Claude extraction latency",
    ["model"],
    buckets=(0.25, 0.5, 0.75, 1, 1.5, 2, 2.5, 4),
)
# What drives the bill: uncached input, cache reads (0.1x), cache writes (1.25x), output.
LLM_TOKENS = Counter("assist_llm_tokens_total", "Claude tokens by kind", ["model", "kind"])


class Intent(StrEnum):
    CARD_FRAUD = "card_fraud"  # lost/stolen card, unrecognised payment
    COMPLAINT_CHASE = "complaint_chase"  # chasing an existing complaint
    PAYMENT_MISSING = "payment_missing"  # sent payment hasn't arrived
    BALANCE_QUERY = "balance_query"
    NEW_COMPLAINT = "new_complaint"


class Vulnerability(StrEnum):
    BEREAVEMENT = "bereavement"
    FINANCIAL_DIFFICULTY = "financial_difficulty"
    HEALTH = "health"
    CAPABILITY = "capability"  # confusion, difficulty understanding


class Risk(StrEnum):
    THIRD_PARTY = "third_party_request"  # asks about someone else's account
    INSTRUCTION_INJECTION = "instruction_injection"  # tries to instruct the assistant
    SENSITIVE_DATA = "sensitive_data_request"  # asks for full card number, PIN, password


class Signals(BaseModel):
    """What one utterance tells us. Every field is optional: most utterances say little."""

    full_name: str | None = Field(None, description="Caller's own name if they state it")
    last_name: str | None = None
    postcode: str | None = Field(None, description="Full UK postcode if stated, e.g. 'SW1A 1AA'")
    customer_id: str | None = Field(None, pattern=r"^C\d{7}$")
    amounts: list[Decimal] = Field(default_factory=list, description="Money amounts mentioned, in GBP")
    intents: list[Intent] = Field(default_factory=list)
    vulnerabilities: list[Vulnerability] = Field(default_factory=list)
    risks: list[Risk] = Field(default_factory=list)

    def merge(self, other: Signals) -> Signals:
        """Union of both, preferring values already present (rules win on conflicts)."""
        return Signals(
            full_name=self.full_name or other.full_name,
            last_name=self.last_name or other.last_name,
            postcode=self.postcode or other.postcode,
            customer_id=self.customer_id or other.customer_id,
            amounts=sorted(set(self.amounts) | set(other.amounts)),
            intents=_union(self.intents, other.intents),
            vulnerabilities=_union(self.vulnerabilities, other.vulnerabilities),
            risks=_union(self.risks, other.risks),
        )


def _union(a: list, b: list) -> list:
    return list(dict.fromkeys([*a, *b]))


# ------------------------------------------------------------------ rules
POSTCODE = re.compile(r"\b([A-Z]{1,2}\d[A-Z\d]?)\s*(\d[A-Z]{2})\b", re.IGNORECASE)
MONEY = re.compile(r"£\s?(\d{1,3}(?:,\d{3})*(?:\.\d{1,2})?|\d+(?:\.\d{1,2})?)")
CUSTOMER_ID = re.compile(r"\b(C\d{7})\b")
NAME = re.compile(
    # Case-insensitive lead-in, case-sensitive name: "it's Sarah Jones", not "it's the one".
    r"\b(?i:my name is|my name's|it's|this is|i'm|i am|name is)\s+"
    r"(?:(?i:mr|mrs|ms|miss|dr)\.?\s+)?([A-Z][A-Za-z'\-]+\b(?:\s+[A-Z][A-Za-z'\-]+\b)+)"
)
NOT_NAMES = {"Sorry", "Just", "Really", "Calling", "Very", "Not", "Still", "About", "Quite"}

INTENT_PATTERNS: dict[Intent, re.Pattern[str]] = {
    Intent.CARD_FRAUD: re.compile(
        r"\b(stolen|lost my (?:debit |credit )?card|card (?:was|has been|got) (?:stolen|taken)|"
        r"don'?t recogni[sz]e|didn'?t make (?:this|that|a) (?:payment|transaction)|unrecogni[sz]ed|"
        r"fraud(?:ulent)?|scam(?:med)?|not me who|(?:someone|somebody) (?:has )?used my card|didn'?t make\b|"
        r"(?:purse|wallet|bag|handbag|phone) (?:was|has been|got) (?:stolen|nicked|taken|pinched))",
        re.IGNORECASE,
    ),
    Intent.COMPLAINT_CHASE: re.compile(
        r"\b(complain(?:ed|t)\b.*\b(?:weeks?|months?|ago|nothing|no one|nobody|still)|"
        r"(?:chas(?:e|ing)|follow(?:ing)? up|update on) (?:my|the) complaint|heard nothing|"
        r"still waiting (?:for|on) (?:a|my) (?:response|reply|answer))",
        re.IGNORECASE,
    ),
    Intent.PAYMENT_MISSING: re.compile(
        r"\b((?:hasn'?t|has not|haven'?t|didn'?t|not) (?:arrived|received|gone through|go through|landed|come through)|"
        r"(?:payment|transfer|money) (?:is )?missing|where(?:'s| is) (?:my|the) (?:payment|transfer|money))",
        re.IGNORECASE,
    ),
    Intent.BALANCE_QUERY: re.compile(r"\b(balance|how much (?:is|have i got) in)", re.IGNORECASE),
    Intent.NEW_COMPLAINT: re.compile(
        r"\b(i want to (?:make|raise|log) a complaint|i'?d like to complain|this is unacceptable)",
        re.IGNORECASE,
    ),
}
VULNERABILITY_PATTERNS: dict[Vulnerability, re.Pattern[str]] = {
    Vulnerability.BEREAVEMENT: re.compile(
        r"\b(passed away|died|death of|bereave\w*|funeral|lost my (?:husband|wife|partner|mum|mother|dad|father|son|daughter))",
        re.IGNORECASE,
    ),
    Vulnerability.FINANCIAL_DIFFICULTY: re.compile(
        r"\b(can'?t (?:afford|pay)|struggling (?:to pay|with (?:money|bills|payments))|behind on|"
        r"lost my job|(?:in|into) debt|bailiffs?|payday loan)",
        re.IGNORECASE,
    ),
    Vulnerability.HEALTH: re.compile(
        r"\b(hospital|diagnos\w+|cancer|chemo\w*|dementia|mental health|anxiety|depress\w+|carer)",
        re.IGNORECASE,
    ),
    Vulnerability.CAPABILITY: re.compile(
        r"\b(i don'?t understand|confus\w+|can you (?:say|explain) that again|not good with (?:computers|technology|online))",
        re.IGNORECASE,
    ),
}
RISK_PATTERNS: dict[Risk, re.Pattern[str]] = {
    Risk.THIRD_PARTY: re.compile(
        r"\b(my (?:husband|wife|partner|son|daughter|mum|mother|dad|father|brother|sister|neighbou?r)'?s? "
        r"(?:account|balance|card|statement)|(?:his|her|their) (?:account|balance|statement))",
        re.IGNORECASE,
    ),
    Risk.INSTRUCTION_INJECTION: re.compile(
        r"\b(ignore (?:all |any |your |the )?(?:previous |prior )?instructions|system prompt|you are now|"
        r"developer mode|as an ai|disregard (?:your|the) (?:rules|policy))",
        re.IGNORECASE,
    ),
    Risk.SENSITIVE_DATA: re.compile(
        r"\b(full (?:card|account) number|(?:my|the) pin\b|password|security code|cvv|one[- ]time (?:passcode|code))",
        re.IGNORECASE,
    ),
}


class RulesExtractor:
    name = "rules"

    def trace(self, text: str, speaker: str) -> tuple[Signals, dict[str, Any]]:
        return self.extract(text, speaker), {"engine": "rules"}

    def extract(self, text: str, speaker: str) -> Signals:
        s = Signals()
        # Identity and intents come from the customer; the colleague's words are context only.
        if speaker != "customer":
            return s
        if m := NAME.search(text):
            parts = [p for p in m.group(1).split() if p not in NOT_NAMES]
            if len(parts) >= 2:
                s.full_name, s.last_name = " ".join(parts), parts[-1]
        if m := POSTCODE.search(text):
            s.postcode = f"{m.group(1).upper()} {m.group(2).upper()}"
        if m := CUSTOMER_ID.search(text):
            s.customer_id = m.group(1)
        s.amounts = [Decimal(a.replace(",", "")) for a in MONEY.findall(text)]
        s.intents = [i for i, p in INTENT_PATTERNS.items() if p.search(text)]
        s.vulnerabilities = [v for v, p in VULNERABILITY_PATTERNS.items() if p.search(text)]
        s.risks = [r for r, p in RISK_PATTERNS.items() if p.search(text)]
        return s


# ------------------------------------------------------------------ LLM
class ClaudeExtractor:
    """Structured extraction with Claude via a forced tool call; falls back to rules on any failure."""

    name = "claude+rules"
    # Label names alone are ambiguous (evals showed "What's my PIN?" tagged balance_query), so
    # the guide defines each label with worked examples. It is also long enough (4k+ tokens
    # with the tool schema) for Haiku's prompt cache: repeat calls read it at 0.1x the price.
    SYSTEM = (Path(__file__).parent / "label_guide.md").read_text()

    def __init__(
        self,
        model: str | None = None,
        timeout_s: float = 2.5,
        client: Any = None,
        budget: SpendBudget | None = None,
    ) -> None:
        import anthropic  # optional at runtime: only needed when a key is configured

        self.budget = budget or BUDGET
        # No retries: a live call can't wait; the rules result is already in hand.
        self.client = client or anthropic.Anthropic(timeout=timeout_s, max_retries=0)
        self.model = model or os.environ.get("CALL_ASSIST_MODEL", "claude-haiku-4-5")
        self.rules = RulesExtractor()
        schema = Signals.model_json_schema()
        self.tool = {
            "name": "record_signals",
            "description": "Record the structured signals found in the utterance.",
            "input_schema": schema,
        }

    def extract(self, text: str, speaker: str) -> Signals:
        return self.trace(text, speaker)[0]

    def trace(self, text: str, speaker: str) -> tuple[Signals, dict[str, Any]]:
        import anthropic

        base = self.rules.extract(text, speaker)
        if speaker != "customer":
            return base, {"engine": "rules", "note": "colleague lines use rules only"}
        started = time.monotonic()
        info: dict[str, Any] = {"model": self.model}
        if not self.budget.allow():
            return self._fallback(base, info, started, "budget", self.budget.reason())
        try:
            msg = self.client.messages.create(
                model=self.model,
                max_tokens=400,
                # Tools then system form the cached prefix; only the utterance is new each call.
                system=[{"type": "text", "text": self.SYSTEM, "cache_control": {"type": "ephemeral"}}],
                tools=[self.tool],
                tool_choice={"type": "tool", "name": "record_signals"},
                messages=[{"role": "user", "content": f"<utterance>{text}</utterance>"}],
            )
            block = next(b for b in msg.content if getattr(b, "type", "") == "tool_use")
            raw, dropped = _known_labels(block.input)
            found = Signals.model_validate(raw)
        # The model is an enhancement, never a dependency: every failure falls back to
        # rules, but says which failure, so a bad key isn't mistaken for a quiet model.
        except anthropic.APITimeoutError:
            return self._fallback(base, info, started, "timeout", "timeout")
        except anthropic.AuthenticationError:
            return self._fallback(base, info, started, "auth", "invalid API key")
        except anthropic.PermissionDeniedError:
            return self._fallback(base, info, started, "permission", "API key lacks permission")
        except anthropic.NotFoundError:
            return self._fallback(base, info, started, "not_found", f"unknown model {self.model}")
        except anthropic.RateLimitError:
            return self._fallback(base, info, started, "rate_limited", "rate limited")
        except anthropic.APIStatusError as exc:
            # Say what the API said (e.g. "credit balance is too low"), not just the code.
            body = exc.body if isinstance(exc.body, dict) else {}
            detail = (body.get("error") or {}).get("message") or ""
            reason = f"API error {exc.status_code}" + (f": {detail[:120]}" if detail else "")
            return self._fallback(base, info, started, "api_error", reason)
        except anthropic.APIConnectionError:
            return self._fallback(base, info, started, "network", "network error")
        except (StopIteration, ValueError) as exc:  # no tool call, or output failed the schema
            detail = " ".join(str(exc).split())[:120]
            return self._fallback(base, info, started, "bad_output", f"unusable output: {detail}")
        elapsed = time.monotonic() - started
        LLM_CALLS.labels(self.model, "ok").inc()
        LLM_LATENCY.labels(self.model).observe(elapsed)
        added = {
            field: [str(x) for x in getattr(found, field) if x not in getattr(base, field)]
            for field in ("intents", "vulnerabilities", "risks")
        }
        info.update(
            engine="claude",
            model=msg.model,
            ms=int(elapsed * 1000),
            request_id=getattr(msg, "_request_id", None),
            input_tokens=msg.usage.input_tokens,
            output_tokens=msg.usage.output_tokens,
            cache_read_tokens=getattr(msg.usage, "cache_read_input_tokens", None) or 0,
            cache_write_tokens=getattr(msg.usage, "cache_creation_input_tokens", None) or 0,
            cost_usd=round(self.budget.record(msg.model, msg.usage), 6),
            added={k: v for k, v in added.items() if v},
        )
        if not (info["cache_read_tokens"] or info["cache_write_tokens"]):
            # Silent below the model's minimum cacheable length: every call pays full price.
            log.warning(
                "Claude prompt not cached (%s input tokens): label guide too short?", info["input_tokens"]
            )
        for kind in ("input", "output", "cache_read", "cache_write"):
            LLM_TOKENS.labels(self.model, kind).inc(info[f"{kind}_tokens"])
        if dropped:
            info["dropped"] = dropped
        return base.merge(found), info

    def _fallback(
        self, base: Signals, info: dict[str, Any], started: float, outcome: str, reason: str
    ) -> tuple[Signals, dict[str, Any]]:
        LLM_CALLS.labels(self.model, outcome).inc()
        log.warning("Claude extraction fell back to rules: %s", reason)
        ms = int((time.monotonic() - started) * 1000)
        return base, info | {"engine": "rules", "fallback": reason, "ms": ms}


LABELS: dict[str, frozenset[str]] = {
    "intents": frozenset(Intent),
    "vulnerabilities": frozenset(Vulnerability),
    "risks": frozenset(Risk),
}


def _known_labels(raw: Any) -> tuple[Any, list[str]]:
    """Drop labels the model invented, so one made-up label doesn't discard the valid ones.

    Returns the cleaned input and what was dropped (shown in the trace). Anything else
    malformed is left for schema validation to reject.
    """
    if not isinstance(raw, dict):
        return raw, []
    clean, dropped = dict(raw), []
    for field, known in LABELS.items():
        values = raw.get(field)
        if isinstance(values, list):
            clean[field] = [v for v in values if v in known]
            dropped += [f"{field}:{v}" for v in values if v not in known]
    return clean, dropped


def default_extractor() -> RulesExtractor | ClaudeExtractor:
    if os.environ.get("ANTHROPIC_API_KEY"):
        try:
            return ClaudeExtractor()
        except ImportError:
            log.warning("anthropic SDK missing; using rules extractor")
    return RulesExtractor()
