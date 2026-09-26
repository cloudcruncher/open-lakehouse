import re
from pathlib import Path
from types import SimpleNamespace

import anthropic
import httpx2
import yaml

from lakehouse_platform.call_assist import signals
from lakehouse_platform.call_assist.signals import ClaudeExtractor, Intent, Vulnerability


class FakeMessages:
    def __init__(self, result):
        self.result, self.calls = result, 0

    def create(self, **kwargs):
        self.calls, self.kwargs = self.calls + 1, kwargs
        assert kwargs["tool_choice"] == {"type": "tool", "name": "record_signals"}
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


def claude(result) -> ClaudeExtractor:
    fake = SimpleNamespace(messages=FakeMessages(result))
    return ClaudeExtractor(model="claude-haiku-4-5", client=fake)


def reply(signals: dict, blocks=None):
    content = blocks if blocks is not None else [SimpleNamespace(type="tool_use", input=signals)]
    return SimpleNamespace(
        content=content,
        model="claude-haiku-4-5",
        usage=SimpleNamespace(input_tokens=412, output_tokens=38),
        _request_id="req_test",
    )


def test_claude_trace_reports_model_tokens_and_what_it_added():
    # Rules catch the bereavement; only the model hears "my card's gone" as card fraud.
    ex = claude(reply({"intents": ["card_fraud"], "vulnerabilities": ["bereavement"]}))
    sig, trace = ex.trace("my card's gone and I lost my mum last week, it's all too much", "customer")
    assert Intent.CARD_FRAUD in sig.intents and Vulnerability.BEREAVEMENT in sig.vulnerabilities
    assert trace["engine"] == "claude" and trace["model"] == "claude-haiku-4-5"
    assert trace["request_id"] == "req_test" and trace["input_tokens"] == 412
    assert trace["added"] == {"intents": ["card_fraud"]}


def test_bad_key_is_named_not_silent():
    req = httpx2.Request("POST", "https://api.anthropic.com/v1/messages")
    resp = httpx2.Response(401, request=req)
    err = anthropic.AuthenticationError("invalid x-api-key", response=resp, body=None)
    sig, trace = claude(err).trace("my card was stolen", "customer")
    assert Intent.CARD_FRAUD in sig.intents  # rules still answered
    assert trace["engine"] == "rules" and trace["fallback"] == "invalid API key"


def test_output_without_a_tool_call_falls_back():
    _, trace = claude(reply({}, blocks=[SimpleNamespace(type="text", text="hi")])).trace("hello", "customer")
    assert trace["fallback"].startswith("unusable output")


def test_colleague_lines_never_reach_the_model():
    ex = claude(reply({}))
    _, trace = ex.trace("Can I take your full name please?", "colleague")
    assert ex.client.messages.calls == 0 and trace["engine"] == "rules"


def test_api_error_carries_the_apis_own_message():
    req = httpx2.Request("POST", "https://api.anthropic.com/v1/messages")
    error = {"type": "invalid_request_error", "message": "Your credit balance is too low"}
    body = {"type": "error", "error": error}
    err = anthropic.BadRequestError("400", response=httpx2.Response(400, request=req), body=body)
    _, trace = claude(err).trace("my card was stolen", "customer")
    assert trace["fallback"] == "API error 400: Your credit balance is too low"


def test_invented_label_is_dropped_not_the_whole_answer():
    ex = claude(reply({"intents": ["card_fraud", "account_closure"], "vulnerabilities": ["bereavement"]}))
    sig, trace = ex.trace("my card's gone and I lost my mum last week", "customer")
    assert sig.intents == [Intent.CARD_FRAUD] and Vulnerability.BEREAVEMENT in sig.vulnerabilities
    assert trace["engine"] == "claude" and trace["dropped"] == ["intents:account_closure"]


def test_label_guide_examples_are_not_eval_cases():
    # The guide's worked examples are in the prompt; eval lines must stay unseen, or the eval
    # would mark its own homework.
    guide = re.findall(r'^"(.+)"$', ClaudeExtractor.SYSTEM, re.MULTILINE)
    evals = Path(signals.__file__).parent / "evals" / "utterances.yaml"
    seen = {u["text"] for u in yaml.safe_load(evals.read_text())} & set(guide)
    assert len(guide) > 100 and not seen


def test_prompt_is_cached_and_cache_use_is_traced():
    usage = SimpleNamespace(
        input_tokens=430, output_tokens=38, cache_read_input_tokens=4372, cache_creation_input_tokens=0
    )
    msg = SimpleNamespace(**{**vars(reply({"intents": ["card_fraud"]})), "usage": usage})
    ex = claude(msg)
    _, trace = ex.trace("my card was nicked", "customer")
    assert trace["cache_read_tokens"] == 4372 and trace["cache_write_tokens"] == 0
    assert ex.client.messages.kwargs["system"][0]["cache_control"] == {"type": "ephemeral"}
