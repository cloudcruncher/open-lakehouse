# 13. Ask the assistant: the model routes, governed code answers

**Status:** accepted as a pilot (not yet proven in `make verify`, see next-steps). Extends [ADR 7](0007-live-call-assist-model-proposes-policy-disposes.md)
and [ADR 12](0012-ai-call-note-and-a-spend-cap.md).

## Context
Colleagues want to ask during a call: "how fast must we refund an unauthorised payment?",
"any open complaints?". A general chat agent with tools would answer both, but it would
send customer data to the model, let the model pick customers and arguments, and answer
in free text that no check covers. ADR 7 rules all of that out.

## Decision
- **One model call per question, forced to call exactly one tool.**
  - `answer_from_procedures`: the model answers in at most three sentences from the
    procedures retrieved for the question (BM25, top 3) and cites their ids. Procedure
    text is bank guidance, not customer data. The answer must cite a procedure it was
    given and pass the grounding check against the cited text, or it is discarded.
  - `look_up`: the model only names which fixed lookup answers the question
    (unrecognised payment, open complaint, sent payment status, balances). Code runs the
    same handler a spoken intent would, as the colleague, for the customer already
    identified on the call, with the same ID&V lock and grounding. The model never sees
    the result and never chooses a customer or arguments.
  - `cannot_answer`: said plainly.
- **Always available:** no key, a spent budget, an API error or a discarded answer falls
  back to plain search: the best-matching procedure's excerpt. The card says which.
- **Cost:** Haiku 4.5 (`CALL_ASSIST_ASK_MODEL`), about 1.5k tokens in and under 0.1k out,
  about $0.002 per question, under the ADR 12 daily cap.

## Consequences
- The assistant answers procedure questions in plain words and brings up the right data
  card on request, without widening what the model sees.
- It cannot answer questions that need reasoning over the data itself ("is this spending
  unusual for her?"). That would need the model to see masked data: a separate decision.
- Questions and answers are cards in the call's stream, so they appear in the console
  like any other guidance; lookups are audited under the call reference as before.
