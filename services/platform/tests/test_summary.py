import asyncio
from datetime import date
from types import SimpleNamespace

from lakehouse_platform.call_assist.budget import SpendBudget, cost_usd
from lakehouse_platform.call_assist.engine import CallSession, Card, Utterance
from lakehouse_platform.call_assist.knowledge import ProcedureIndex
from lakehouse_platform.call_assist.signals import ClaudeExtractor, RulesExtractor
from lakehouse_platform.call_assist.summary import ClaudeSummarizer


class FakeMessages:
    def __init__(self, text):
        self.text, self.calls, self.kwargs = text, 0, None

    def create(self, **kwargs):
        self.calls, self.kwargs = self.calls + 1, kwargs
        return SimpleNamespace(
            content=[SimpleNamespace(type="text", text=self.text)],
            model="claude-haiku-4-5",
            usage=SimpleNamespace(input_tokens=900, output_tokens=120),
            _request_id="req_sum",
        )


def summarizer(text, budget=None):
    fake = SimpleNamespace(messages=FakeMessages(text))
    return ClaudeSummarizer(client=fake, budget=budget or SpendBudget(1.0))


def ended_call(summ):
    async def emit(_):
        return None

    s = CallSession("CALL-1", "alice", emit, RulesExtractor(), ProcedureIndex.default(), summarizer=summ)
    s.customer = {
        "customer_id": "C0000042",
        "first_name": "Priya",
        "last_name": "Shah",
        "phone": "07700900123",
    }
    s.cards = [
        Card("identity", "Likely caller: Priya Shah (C0000042)", "Verify first"),
        Card(
            "action",
            "Unrecognised payment located",
            "£89.99 to QuickShop on 2026-09-25",
            evidence=[{"amount": 89.99}],
        ),
    ]

    async def run():
        await s.on_utterance(
            Utterance("CALL-1", "alice", 1, "customer", "There's a payment of £89.99 I didn't make.")
        )
        return await s.end()

    return s, asyncio.run(run())


def test_ai_note_is_used_and_model_never_sees_lakehouse_data():
    summ = summarizer("Reason: unrecognised payment of £89.99.\nAgreed: card blocked.\nFollow-up: none")
    _, card = ended_call(summ)
    assert card.ai["engine"] == "claude" and "AI draft" in card.title
    assert card.body.startswith("Caller: Priya Shah (C0000042)")  # written by code, not the model
    assert "Reason: unrecognised payment of £89.99." in card.body and "audited under CALL-1" in card.body
    sent = str(summ.client.messages.kwargs["messages"])
    assert "Unrecognised payment located" in sent  # guidance titles only
    for secret in ("C0000042", "Priya", "Shah", "07700900123", "QuickShop", "2026-09-25"):
        assert secret not in sent


def test_ungrounded_amount_falls_back_to_template():
    _, card = ended_call(summarizer("Reason: refund of £450.00 promised."))
    assert card.ai["engine"] == "template" and "£450.00" in card.ai["fallback"]
    assert "AI draft" not in card.title and "£450" not in card.body


def test_spent_budget_skips_the_model():
    budget = SpendBudget(0.001)
    budget.record("claude-haiku-4-5", {"input_tokens": 2000})  # $0.002 > cap
    summ = summarizer("Reason: x", budget=budget)
    _, card = ended_call(summ)
    assert summ.client.messages.calls == 0
    assert card.ai["fallback"].startswith("daily AI budget reached")


def test_no_summarizer_keeps_the_template():
    _, card = ended_call(None)
    assert card.ai is None and card.title == "Draft call note (check before saving)"


def test_budget_prices_cache_and_resets_daily():
    usage = {
        "input_tokens": 400,
        "cache_read_input_tokens": 4000,
        "cache_creation_input_tokens": 0,
        "output_tokens": 40,
    }
    assert round(cost_usd("claude-haiku-4-5-20251001", usage), 6) == round((400 + 400 + 200) / 1e6, 6)
    assert cost_usd("claude-unknown-9", {"input_tokens": 1000}) > cost_usd(
        "claude-sonnet-5", {"input_tokens": 1000}
    )
    day = [date(2026, 9, 26)]
    b = SpendBudget(0.01, today=lambda: day[0])
    b.record("claude-haiku-4-5", {"input_tokens": 20000})
    assert not b.allow()
    day[0] = date(2026, 9, 27)
    assert b.allow() and b.spent == 0


def test_extractor_respects_the_budget():
    budget = SpendBudget(0.0)
    fake = SimpleNamespace(messages=FakeMessages(""))
    ex = ClaudeExtractor(client=fake, budget=budget)
    sig, trace = ex.trace("my card was stolen", "customer")
    assert fake.messages.calls == 0 and trace["engine"] == "rules"
    assert trace["fallback"].startswith("daily AI budget reached") and sig.intents  # rules still answered
