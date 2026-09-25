"""The assist engine: one session per live call, turning utterances into guidance cards.

    utterance -> extract signals -> plan (fixed policy) -> governed tool calls as the
    colleague -> compose card -> grounding check -> procedure citation -> colleague

Design rules:
  * The colleague stays in control. Cards advise; nothing is changed on the account.
  * Identity before data. Account cards are marked "verify first" until the colleague
    confirms ID&V. Data needed *to verify* (name, postcode, DoB year, phone last 4)
    is shown, so the colleague asks for it rather than reading it out.
  * Policy disposes. Extraction (rules or LLM) only proposes intents; which tools run,
    with what arguments, is fixed code. The model never writes queries or picks
    customers.
  * No unsupported facts. Every card passes the grounding check or is withheld.
  * No credentials of its own. Without the colleague's token the engine only
    listens: data-driven steps wait until the colleague's session attaches.
  * Degrade honestly. If data is unavailable, the card says so. It never guesses.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

from prometheus_client import Counter, Histogram

from .grounding import ungrounded
from .knowledge import ProcedureIndex
from .signals import Intent, Risk, Signals, Vulnerability
from .tools import ToolBackend, ToolFailure

log = logging.getLogger(__name__)

CARDS = Counter("assist_cards_total", "Guidance cards shown", ["kind"])
WITHHELD = Counter("assist_cards_withheld_total", "Cards withheld by the grounding check")
LATENCY = Histogram(
    "assist_guidance_latency_seconds",
    "Utterance received -> card shown",
    ["kind"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 1.5, 2, 3, 5, 8, 13),
)
TOOL_CALLS = Counter("assist_tool_calls_total", "Tool calls made by the engine", ["tool", "outcome"])

Emit = Callable[[dict[str, Any]], Awaitable[None]]
PRIORITY = {"guard": 0, "compliance": 1, "action": 2, "identity": 3, "insight": 4, "status": 5, "summary": 6}
FRESH_RETRIES = 3  # the customer may be calling about something that happened seconds ago


@dataclass
class Utterance:
    call_id: str
    colleague: str
    seq: int
    speaker: str  # customer | colleague
    text: str
    ts: float = field(default_factory=time.time)


@dataclass
class Card:
    kind: str
    title: str
    body: str
    evidence: list[dict[str, Any]] = field(default_factory=list)
    procedure: dict[str, str] | None = None
    requires_verification: bool = False
    utterance_seq: int | None = None
    latency_ms: int | None = None
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:10])

    @property
    def priority(self) -> int:
        return PRIORITY[self.kind]


def money(v: Any) -> str:
    return f"£{abs(Decimal(str(v))):,.2f}"


def day(v: Any) -> str:
    return str(v)[:10]


def add_business_days(start: date, n: int) -> date:
    d = start
    while n > 0:
        d += timedelta(days=1)
        if d.weekday() < 5:
            n -= 1
    return d


def complaint_deadline(opened: date, category: str) -> tuple[date, str]:
    """Final-response deadline under DISP: 15 business days for payment services, else 8 weeks."""
    if category in ("payments", "fraud_scam"):
        return add_business_days(opened, 15), "15 business days (payment services)"
    return opened + timedelta(weeks=8), "8 weeks"


class CallSession:
    def __init__(
        self,
        call_id: str,
        colleague: str,
        emit: Emit,
        extractor: Any,
        index: ProcedureIndex,
        tools: ToolBackend | None = None,
        today: Callable[[], date] = lambda: datetime.now(UTC).date(),
        fresh_retry_delay_s: float = 4.0,
    ) -> None:
        self.call_id = call_id
        self.colleague = colleague
        self.emit_raw = emit
        self.extractor = extractor
        self.index = index
        self.tools = tools
        self.today = today
        self.fresh_retry_delay_s = fresh_retry_delay_s
        self.signals = Signals()
        self.transcript: list[Utterance] = []
        self.cards: list[Card] = []
        self.customer: dict[str, Any] | None = None
        self.accounts: list[dict[str, Any]] = []
        self.verified = False
        self.handled: set[str] = set()
        self.tool_log: list[dict[str, Any]] = []
        self.ended = False
        self._lock = asyncio.Lock()
        self._pending: Utterance | None = None

    # ------------------------------------------------------------ public API
    async def on_utterance(self, u: Utterance) -> None:
        async with self._lock:
            received = time.monotonic()
            self.transcript.append(u)
            await self.emit(
                {"type": "transcript", "seq": u.seq, "speaker": u.speaker, "text": u.text, "ts": u.ts}
            )
            sig = await asyncio.to_thread(self.extractor.extract, u.text, u.speaker)
            self.signals = self.signals.merge(sig)
            if sig.intents or sig.vulnerabilities or sig.risks:
                await self.emit(
                    {
                        "type": "signals",
                        "seq": u.seq,
                        "intents": [str(i) for i in sig.intents],
                        "vulnerabilities": [str(v) for v in sig.vulnerabilities],
                        "risks": [str(r) for r in sig.risks],
                        "extractor": getattr(self.extractor, "name", "rules"),
                    }
                )
            await self._no_data_cards(sig, u, received)
            if self.tools is None:
                self._pending = u
                return
            await self._advance(u, received)

    async def attach_tools(self, tools: ToolBackend) -> None:
        """The colleague's console connected: data-driven steps can now run as them."""
        async with self._lock:
            self.tools = tools
            if self._pending is not None:
                u, self._pending = self._pending, None
                await self._advance(u, time.monotonic())

    async def mark_verified(self) -> None:
        async with self._lock:
            self.verified = True
            await self.emit({"type": "verified"})

    async def end(self) -> Card:
        async with self._lock:
            self.ended = True
            card = self._wrap_up()
            await self._show(card, None, time.monotonic())
            await self.emit({"type": "ended"})
            return card

    async def emit(self, event: dict[str, Any]) -> None:
        await self.emit_raw({"call_id": self.call_id, **event})

    # --------------------------------------------------------------- planning
    async def _no_data_cards(self, sig: Signals, u: Utterance, received: float) -> None:
        for risk in sig.risks:
            if f"risk:{risk}" in self.handled:
                continue
            self.handled.add(f"risk:{risk}")
            title, body, proc = {
                Risk.THIRD_PARTY: (
                    "Third-party request",
                    "The caller is asking about someone else's account. Don't confirm or disclose anything "
                    "without a registered authority (mandate or power of attorney).",
                    "DATA-3P-01",
                ),
                Risk.INSTRUCTION_INJECTION: (
                    "Caller tried to instruct the assistant",
                    "What the caller said was treated as information, not an instruction. Nothing was revealed "
                    "or bypassed. Carry on with the call as normal; the attempt is logged for review.",
                    "SAFETY-01",
                ),
                Risk.SENSITIVE_DATA: (
                    "Sensitive data mentioned",
                    "Never ask for or repeat a full card number, PIN, CVV, password or one-time passcode.",
                    "SAFETY-01",
                ),
            }[risk]
            await self._show(Card("guard", title, body, procedure=self._proc(proc)), u, received)

        for v in sig.vulnerabilities:
            if f"vuln:{v}" in self.handled:
                continue
            self.handled.add(f"vuln:{v}")
            title, body, proc = {
                Vulnerability.BEREAVEMENT: (
                    "Bereavement disclosed",
                    "Slow down and acknowledge the loss. Offer the Bereavement Support team or a call back. "
                    "Ask for consent to record this so they don't have to repeat it.",
                    "VULN-BEREAVEMENT-01",
                ),
                Vulnerability.FINANCIAL_DIFFICULTY: (
                    "Possible financial difficulty",
                    "Acknowledge it without judgement. Offer breathing space on fees and collections, and free "
                    "debt advice. Don't offer new credit on this call.",
                    "VULN-FINDIFF-01",
                ),
                Vulnerability.HEALTH: (
                    "Health circumstances mentioned",
                    "Adapt the call: plain language, check understanding, offer adjustments. Record with consent.",
                    "VULN-HEALTH-01",
                ),
                Vulnerability.CAPABILITY: (
                    "Caller may need more support",
                    "Slow down and check understanding. Avoid high-risk changes on this call; offer to follow up "
                    "in writing.",
                    "VULN-HEALTH-01",
                ),
            }[v]
            await self._show(Card("compliance", title, body, procedure=self._proc(proc)), u, received)

    async def _advance(self, u: Utterance, received: float) -> None:
        if self.customer is None and "identify" not in self.handled:
            await self._identify(u, received)
        if self.customer is None:
            return
        for intent in self.signals.intents:
            # Amount-driven intents run again once the caller names an amount: the first
            # pass lists candidates, the second pinpoints the exact payment.
            refinable = intent in (Intent.CARD_FRAUD, Intent.PAYMENT_MISSING)
            key = f"intent:{intent}" + (f":{bool(self.signals.amounts)}" if refinable else "")
            if key in self.handled:
                continue
            self.handled.add(key)
            handler = {
                Intent.CARD_FRAUD: self._card_fraud,
                Intent.COMPLAINT_CHASE: self._complaint_chase,
                Intent.PAYMENT_MISSING: self._payment_missing,
                Intent.BALANCE_QUERY: self._balances,
                Intent.NEW_COMPLAINT: self._new_complaint,
            }[intent]
            await handler(u, received)

    async def _identify(self, u: Utterance, received: float) -> None:
        s = self.signals
        if s.customer_id:
            candidate_id = s.customer_id
        elif s.last_name and s.postcode:
            self.handled.add("identify")
            res = await self._tool(
                "find_customer",
                {"last_name": s.last_name, "postcode_outward": s.postcode.split()[0]},
                u,
                received,
            )
            if res is None:
                return
            matches = res["matches"]
            first = (s.full_name or "").split()[0].lower() if s.full_name else None
            if first and len(matches) > 1:
                matches = [m for m in matches if (m.get("first_name") or "").lower() == first] or matches
            if not matches:
                await self._show(
                    Card(
                        "identity",
                        "No matching customer you can serve",
                        f"No customer named {s.last_name} in {s.postcode.split()[0]} is visible to you. They may "
                        "bank with another brand; offer a transfer rather than searching further.",
                        evidence=[{"tool": "find_customer", "data": res}],
                        procedure=self._proc("IDV-01"),
                    ),
                    u,
                    received,
                )
                return
            if len(matches) > 1:
                await self._show(
                    Card(
                        "identity",
                        f"{len(matches)} possible customers",
                        "Several customers match. Ask for the date of birth to tell them apart.",
                        evidence=[{"tool": "find_customer", "data": res}],
                        procedure=self._proc("IDV-01"),
                    ),
                    u,
                    received,
                )
                return
            candidate_id = matches[0]["customer_id"]
        else:
            return

        self.handled.add("identify")
        # Fetch the profile and live accounts in parallel: the colleague is waiting.
        p360, accts = await asyncio.gather(
            self._tool("get_customer_360", {"customer_id": candidate_id}, u, received),
            self._tool("get_accounts", {"customer_id": candidate_id}, u, received),
        )
        if p360 is None:
            return
        if not p360.get("found"):
            await self._show(
                Card(
                    "identity",
                    "Customer not visible to you",
                    p360.get("note", ""),
                    procedure=self._proc("IDV-01"),
                ),
                u,
                received,
            )
            return
        profile = p360["profile"]
        self.customer = profile
        self.accounts = (accts or {}).get("accounts", [])
        await self.emit(
            {
                "type": "customer",
                "profile": profile,
                "accounts": self.accounts,
                "data_as_of": p360.get("data_as_of"),
            }
        )

        checks = []
        if profile.get("date_of_birth"):
            dob = day(profile["date_of_birth"])
            checks.append(f"year of birth {dob[:4]}" if dob.endswith("-01-01") else f"date of birth {dob}")
        phone = profile.get("phone") or ""
        if phone[-4:].isdigit():
            checks.append(f"phone ending {phone[-4:]}")
        name = f"{profile.get('first_name', '')} {profile.get('last_name', '')}".strip()
        await self._show(
            Card(
                "identity",
                f"Likely caller: {name} ({profile['customer_id']})",
                "Verify before discussing the account: ask the caller to confirm their "
                + (" and ".join(checks) if checks else "security details")
                + ". Don't read these out.",
                evidence=[{"tool": "get_customer_360", "data": {k: profile.get(k) for k in ("customer_id", "date_of_birth", "phone")}}],
                procedure=self._proc("IDV-01"),
            ),
            u,
            received,
        )
        if profile.get("vulnerability_flag"):
            await self._show(
                Card(
                    "compliance",
                    "Vulnerability recorded on file",
                    "This customer has a recorded vulnerability. Adapt the call and check what support they've "
                    "asked for before.",
                    evidence=[{"tool": "get_customer_360", "fields": ["vulnerability_flag"]}],
                    procedure=self._proc("VULN-HEALTH-01"),
                ),
                u,
                received,
            )
        if profile.get("open_complaints"):
            await self._show(
                Card(
                    "insight",
                    f"{profile['open_complaints']} open complaint(s)",
                    f"Most recent: {profile.get('last_complaint_category', 'unknown')}, status "
                    f"{profile.get('last_complaint_status', 'unknown')}. Acknowledge it if they raise it.",
                    evidence=[
                        {
                            "tool": "get_customer_360",
                            "data": {
                                k: profile.get(k)
                                for k in (
                                    "open_complaints",
                                    "last_complaint_category",
                                    "last_complaint_status",
                                )
                            },
                        }
                    ],
                    requires_verification=True,
                ),
                u,
                received,
            )
        frozen = [a for a in self.accounts if a.get("status") == "frozen"]
        if frozen:
            await self._show(
                Card(
                    "insight",
                    "Account frozen",
                    ", ".join(f"{a['product'].replace('_', ' ')} {a['account_id']}" for a in frozen)
                    + " is frozen: payments from it will fail.",
                    evidence=[{"tool": "get_accounts", "data": frozen}],
                    requires_verification=True,
                ),
                u,
                received,
            )

    # ---------------------------------------------------------------- intents
    async def _recent(self, u: Utterance, received: float, days: int, want: set[Decimal]) -> dict | None:
        """Recent transactions; if the caller named an amount we can't see yet, wait for the stream."""
        cid = self.customer["customer_id"]
        res = None
        for attempt in range(FRESH_RETRIES):
            res = await self._tool(
                "get_recent_transactions", {"customer_id": cid, "days": days, "limit": 30}, u, received
            )
            if res is None or not want:
                return res
            if any(abs(Decimal(str(t["amount"]))) in want for t in res["transactions"]):
                return res
            if attempt < FRESH_RETRIES - 1:
                await self.emit({"type": "status", "text": "Waiting for the latest transactions to arrive…"})
                await asyncio.sleep(self.fresh_retry_delay_s)
        return res

    async def _card_fraud(self, u: Utterance, received: float) -> None:
        want = {a.quantize(Decimal("0.01")) for a in self.signals.amounts}
        res = await self._recent(u, received, 3, want)
        if res is None:
            return
        txns = res["transactions"]
        match = [t for t in txns if want and abs(Decimal(str(t["amount"]))) in want]
        if match:
            t = match[0]
            ts = str(t["txn_ts"])
            body = (
                f"Found it: {money(t['amount'])} to {t.get('merchant') or 'unknown merchant'} on {day(ts)} at "
                f"{ts[11:16]} UTC, status {t['status']} (transaction {t['txn_id']}). Freeze the card now, then "
                "raise a fraud claim."
                + (
                    " A pending payment can often be stopped before it settles."
                    if t["status"] == "pending"
                    else ""
                )
            )
            card = Card(
                "action",
                "Unrecognised payment located",
                body,
                [{"tool": "get_recent_transactions", "data": t}],
            )
        else:
            card_payments = [t for t in txns if t.get("channel") == "card" and Decimal(str(t["amount"])) < 0][
                :3
            ]
            listing = "; ".join(
                f"{money(t['amount'])} {t.get('merchant') or ''} ({day(t['txn_ts'])})" for t in card_payments
            )
            body = "Freeze the card now. Go through the recent card payments with the caller: " + (
                listing or "none in the last 3 days."
            )
            card = Card(
                "action",
                "Possible card fraud",
                body,
                [{"tool": "get_recent_transactions", "data": card_payments}],
            )
        card.procedure = self._proc("CARD-FRAUD-01")
        card.requires_verification = True
        await self._show(card, u, received)

    async def _complaint_chase(self, u: Utterance, received: float) -> None:
        res = await self._tool("get_complaints", {"customer_id": self.customer["customer_id"]}, u, received)
        if res is None:
            return
        if not res["complaints"]:
            await self._show(
                Card(
                    "insight",
                    "No open complaint found",
                    "No open complaint is recorded. Offer to log one now; it may have been closed already.",
                    [{"tool": "get_complaints", "data": res}],
                    self._proc("COMPLAINTS-DISP-01"),
                    requires_verification=True,
                ),
                u,
                received,
            )
            return
        c = res["complaints"][0]
        opened = date.fromisoformat(day(c["opened_at"]))
        due, rule = complaint_deadline(opened, c["category"])
        left = (due - self.today()).days
        when = f"{left} day(s) left" if left > 0 else "due today" if left == 0 else f"{-left} day(s) overdue"
        body = (
            f"Complaint {c['complaint_id']} ({c['category'].replace('_', ' ')}) opened {opened.isoformat()}, "
            f"status {c['status']}. Final response due {due.isoformat()} ({rule}): {when}."
        )
        if left <= 0:
            body += " Tell the customer they can refer it to the Financial Ombudsman Service now."
        elif left <= 5:
            body += " Flag it to the complaint owner today."
        await self._show(
            Card(
                "action" if left <= 5 else "insight",
                "Complaint deadline",
                body,
                # Derived facts are evidence too, shown with how they were derived.
                [{"tool": "get_complaints", "data": c}, {"derived": {"final_response_due": due.isoformat(), "rule": rule}}],
                self._proc("COMPLAINTS-DISP-01"),
                requires_verification=True,
            ),
            u,
            received,
        )

    async def _payment_missing(self, u: Utterance, received: float) -> None:
        want = {a.quantize(Decimal("0.01")) for a in self.signals.amounts}
        res = await self._recent(u, received, 7, want)
        if res is None:
            return
        outgoing = [
            t
            for t in res["transactions"]
            if Decimal(str(t["amount"])) < 0 and t.get("channel") in ("faster_payment", "online", "mobile")
        ]
        match = [t for t in outgoing if want and abs(Decimal(str(t["amount"]))) in want] or outgoing[:1]
        if not match:
            body = "No outgoing transfer in the last 7 days. Check the account and payee details with the caller."
            evidence: list[dict[str, Any]] = [{"tool": "get_recent_transactions", "data": res}]
        else:
            t = match[0]
            sent = date.fromisoformat(day(t["txn_ts"]))
            age = (self.today() - sent).days
            body = f"{money(t['amount'])} to {t.get('merchant') or 'the payee'} on {sent.isoformat()}, status {t['status']}. "
            if t["status"] == "pending":
                body += "It's still processing, possibly held for a fraud check. Don't promise a time."
            elif age >= 1:
                body += "It left the account more than a business day ago: raise a payment trace."
            else:
                body += "It left the account today: Faster Payments normally arrive within 2 hours."
            evidence = [{"tool": "get_recent_transactions", "data": t}]
        await self._show(
            Card(
                "action",
                "Payment status",
                body,
                evidence,
                self._proc("PAYMENTS-FPS-01"),
                requires_verification=True,
            ),
            u,
            received,
        )

    async def _balances(self, u: Utterance, received: float) -> None:
        if not self.accounts:
            return
        lines = "; ".join(
            f"{a['product'].replace('_', ' ')}: {money(a['balance'])}{' (overdrawn)' if Decimal(str(a['balance'])) < 0 and a['product'] == 'current_account' else ''}"
            for a in self.accounts
            if a.get("status") == "open"
        )
        await self._show(
            Card(
                "insight",
                "Live balances",
                lines or "No open accounts.",
                [{"tool": "get_accounts", "data": self.accounts}],
                requires_verification=True,
            ),
            u,
            received,
        )

    async def _new_complaint(self, u: Utterance, received: float) -> None:
        await self._show(
            Card(
                "action",
                "Log a complaint",
                "Log the complaint now, with the customer's own words. Tell them the timeline: a final response "
                "within 8 weeks, or 15 business days for payment services.",
                procedure=self._proc("COMPLAINTS-DISP-01"),
            ),
            u,
            received,
        )

    # ---------------------------------------------------------------- plumbing
    async def _tool(self, tool: str, args: dict[str, Any], u: Utterance, received: float) -> dict | None:
        args = {"call_reference": self.call_id, **args}
        started = time.monotonic()
        entry: dict[str, Any] = {"type": "tool_call", "tool": tool, "args": args}
        try:
            result = await self.tools.call(tool, args)
            entry.update(outcome="ok", ms=int((time.monotonic() - started) * 1000))
            TOOL_CALLS.labels(tool, "ok").inc()
            return result
        except ToolFailure as exc:
            entry.update(outcome=exc.kind, ms=int((time.monotonic() - started) * 1000), error=str(exc)[:200])
            TOOL_CALLS.labels(tool, exc.kind).inc()
            if f"fail:{exc.kind}" not in self.handled:
                self.handled.add(f"fail:{exc.kind}")
                text = {
                    "denied": "You aren't cleared to see this customer data. The assistant can't see more than you can.",
                    "unavailable": "Customer data is temporarily unavailable. Carry on with the caller and don't "
                    "guess; the assistant will retry on the next thing they say.",
                }.get(exc.kind, "A data lookup failed; the platform team has been alerted.")
                await self._show(Card("status", f"{tool}: {exc.kind}", text), u, received)
            if exc.kind == "unavailable":
                # Allow the step to run again on the next utterance.
                self.handled.discard("identify")
                self.handled = {h for h in self.handled if not h.startswith("intent:")}
                self.handled.discard("fail:unavailable")
            return None
        finally:
            self.tool_log.append(entry)
            await self.emit(entry)

    def _proc(self, proc_id: str) -> dict[str, str] | None:
        p = self.index.by_id(proc_id)
        return {"id": p.id, "title": p.title, "excerpt": p.excerpt()} if p else None

    async def _show(self, card: Card, u: Utterance | None, received: float) -> None:
        if card.requires_verification and self.verified:
            card.requires_verification = False
        transcript = " ".join(t.text for t in self.transcript)
        missing = ungrounded(f"{card.title} {card.body}", card.evidence, transcript)
        if missing:
            WITHHELD.inc()
            log.warning("withheld ungrounded card %r: %s", card.title, missing)
            await self.emit({"type": "withheld", "title": card.title, "unsupported": missing})
            return
        card.utterance_seq = u.seq if u else None
        card.latency_ms = int((time.monotonic() - received) * 1000)
        self.cards.append(card)
        CARDS.labels(card.kind).inc()
        LATENCY.labels(card.kind).observe(card.latency_ms / 1000)
        await self.emit({"type": "card", "card": asdict(card) | {"priority": card.priority}})

    def _wrap_up(self) -> Card:
        """After-call note: drafted for the colleague to check and save, not saved automatically."""
        name = ""
        if self.customer:
            name = f"{self.customer.get('first_name', '')} {self.customer.get('last_name', '')} ({self.customer['customer_id']})"
        reasons = ", ".join(str(i).replace("_", " ") for i in self.signals.intents) or "general enquiry"
        vulns = ", ".join(str(v).replace("_", " ") for v in self.signals.vulnerabilities)
        actions = [c.title for c in self.cards if c.kind in ("action", "compliance", "guard")]
        lines = [
            f"Caller: {name or 'not identified'}. ID&V: {'completed' if self.verified else 'NOT confirmed'}.",
            f"Reason for call: {reasons}.",
        ]
        if vulns:
            lines.append(f"Vulnerability disclosed: {vulns} (record only with consent).")
        if actions:
            lines.append("Guidance given: " + "; ".join(dict.fromkeys(actions)) + ".")
        lines.append(
            f"Data lookups: {sum(1 for t in self.tool_log if t.get('outcome') == 'ok')}, all audited under {self.call_id}."
        )
        return Card(
            "summary",
            "Draft call note (check before saving)",
            " ".join(lines),
            evidence=[{"customer_id": (self.customer or {}).get("customer_id"), "call": self.call_id}],
        )
