# 7. Live Call Assist: the model proposes, fixed policy disposes

**Status:** accepted

## Context
An assistant listening to a live call could, in the naive design, let an LLM decide
which customer data to fetch and write free-text advice. That is non-deterministic,
hard to test, open to prompt injection by the caller, and can state facts that
aren't true, all in a regulated conversation.

## Decision
- **Understanding** (what was said) is the only place a model is used. Rules run
  always. An LLM (Claude, forced tool call with a JSON schema) adds recall when
  `ANTHROPIC_API_KEY` is set, and its output is merged under the rules, never replacing them.
  The caller's words are treated strictly as data. Injection attempts become a signal
  that produces a guard card.
- **Planning** (what to fetch) is fixed code: identity first (name + postcode district),
  then intent-specific tools. The model never writes queries or chooses a customer.
- **Access** is the colleague's own token through the MCP gateway (ADR 3), so the
  assistant sees exactly what the colleague may see, and every lookup is audited
  under the call reference.
- **Output**: every card passes a **grounding check**. Each amount, date and id must
  appear in the evidence (tool results, derived facts with their derivation, or the
  transcript), or the card is withheld and counted. Cards cite a procedure id.
- **Human in control:** account cards stay locked until the colleague confirms ID&V.
  The after-call note is a draft for the colleague to check before saving.

## Consequences
- Deterministic, testable behaviour: offline evals in CI (tool selection, expected
  cards, zero ungrounded cards, forbidden content) plus understanding precision and
  recall with a floor that only goes up.
- Honest limits: the rules extractor misses paraphrases (the eval reports intents at
  93% and vulnerabilities at 78% recall), and that gap is what the LLM layer is for.
- Latency: guidance lands 0.4–1 s after the caller speaks (tool calls dominate).
