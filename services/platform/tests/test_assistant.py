import asyncio
from datetime import date
from types import SimpleNamespace

from lakehouse_platform.call_assist.assistant import ClaudeAssistant
from lakehouse_platform.call_assist.budget import SpendBudget
from lakehouse_platform.call_assist.engine import CallSession
from lakehouse_platform.call_assist.knowledge import ProcedureIndex
from lakehouse_platform.call_assist.signals import RulesExtractor

COMPLAINT = {
    "complaint_id": "CMP0001234",
    "opened_at": "2026-08-03T10:00:00",
    "category": "fees_and_charges",
    "status": "investigating",
}


class FakeMessages:
    def __init__(self, tool, args):
        self.tool, self.args, self.calls, self.kwargs = tool, args, 0, None

    def create(self, **kwargs):
        self.calls, self.kwargs = self.calls + 1, kwargs
        return SimpleNamespace(
            content=[SimpleNamespace(type="tool_use", name=self.tool, input=self.args)],
            model="claude-haiku-4-5",
            usage=SimpleNamespace(input_tokens=700, output_tokens=60),
            _request_id="req_ask",
        )


class Tools:
    def __init__(self):
        self.calls = []

    async def call(self, tool, args):
        self.calls.append(tool)
        return {"complaints": [dict(COMPLAINT)]}

    async def close(self):
        return None


def session(tool=None, args=None, budget=None, identified=True):
    events = []

    async def emit(ev):
        events.append(ev)

    assistant = None
    if tool:
        fake = SimpleNamespace(messages=FakeMessages(tool, args))
        assistant = ClaudeAssistant(client=fake, budget=budget or SpendBudget(1.0))
    s = CallSession(
        "CALL-9",
        "alice",
        emit,
        RulesExtractor(),
        ProcedureIndex.default(),
        Tools(),
        today=lambda: date(2026, 9, 26),
        assistant=assistant,
    )
    if identified:
        s.customer = {"customer_id": "C0000042", "first_name": "Priya", "last_name": "Shah"}
    return s, events


def cards(events):
    return [e["card"] for e in events if e["type"] == "card"]


def test_procedure_answer_cites_and_sees_no_customer_data():
    answer = "Freeze the card first, then raise a fraud claim for each disputed transaction."
    s, events = session("answer_from_procedures", {"answer": answer, "procedure_ids": ["CARD-FRAUD-01"]})
    asyncio.run(s.ask("what do I do about a stolen card?"))
    [card] = cards(events)
    assert card["body"] == answer and card["procedure"]["id"] == "CARD-FRAUD-01"
    assert card["ai"]["engine"] == "claude"
    sent = str(s.assistant.client.messages.kwargs["messages"])
    assert "CARD-FRAUD-01" in sent and "C0000042" not in sent and "Shah" not in sent


def test_data_question_runs_the_governed_lookup_and_model_never_sees_it():
    s, events = session("look_up", {"what": "open_complaint"})
    asyncio.run(s.ask("any open complaints?"))
    titles = [c["title"] for c in cards(events)]
    assert titles == ["You asked: any open complaints?", "Complaint deadline"]
    assert s.tools.calls == ["get_complaints"]  # the fixed handler, as the colleague
    assert s.assistant.client.messages.calls == 1  # one routing call, no second call with the data
    assert "CMP0001234" not in str(s.assistant.client.messages.kwargs)


def test_lookup_needs_an_identified_caller():
    s, events = session("look_up", {"what": "balances"}, identified=False)
    asyncio.run(s.ask("what's their balance?"))
    [card] = cards(events)
    assert "Identify the caller first" in card["body"] and s.tools.calls == []


def test_unsupported_fact_in_answer_falls_back_to_search():
    s, events = session(
        "answer_from_procedures",
        {"answer": "Refunds over £500 need approval.", "procedure_ids": ["CARD-FRAUD-01"]},
    )
    asyncio.run(s.ask("stolen card refund rules"))
    [card] = cards(events)
    assert card["ai"]["engine"] == "search" and "£500" in card["ai"]["fallback"]
    assert "£500" not in card["body"] and card["procedure"]  # the best procedure's excerpt instead


def test_citing_an_unretrieved_procedure_falls_back():
    s, events = session("answer_from_procedures", {"answer": "Do X.", "procedure_ids": ["MADE-UP-01"]})
    asyncio.run(s.ask("stolen card"))
    assert cards(events)[0]["ai"]["engine"] == "search"


def test_no_model_or_spent_budget_uses_search():
    s, events = session()
    asyncio.run(s.ask("the customer's card was stolen"))
    [card] = cards(events)
    assert card["ai"] is None and card["procedure"]["id"] == "CARD-FRAUD-01"
    budget = SpendBudget(0.0)
    s, events = session("look_up", {"what": "balances"}, budget=budget)
    asyncio.run(s.ask("stolen card"))
    assert s.assistant.client.messages.calls == 0
    assert cards(events)[0]["ai"]["fallback"].startswith("daily AI budget reached")
