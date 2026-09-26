# 12. The AI writes the call note from what was said, under a spend cap

**Status:** accepted. Amends [ADR 7](0007-live-call-assist-model-proposes-policy-disposes.md).

## Context
ADR 7 used a model for one thing: labelling each caller line. The after-call note was a
template ("Reason for call: card fraud. Guidance given: ..."), accurate but thin: it
could not say what was agreed, in the customer's terms. Writing that note is a large
share of a contact-centre colleague's after-call work, and it is what a language model
does well. Two risks come with it: the model could see customer data it does not need,
or state a fact that is not true. Separately, each model call costs money, and a demo
running on a small prepaid balance must not drain it.

## Decision
- **At the end of a call, Claude drafts the narrative of the note** (reason, what was
  found, what was agreed, follow-up, any disclosed vulnerability). The colleague checks
  it before saving, as before.
- **The model sees only what was said**: the transcript (both speakers), the titles of
  the guidance shown, the labels detected and whether ID&V was completed. It never sees
  lakehouse data: no card bodies, evidence, customer records, names or ids. The lines
  that come from data (who the caller is, ID&V, the audit reference) are written by code
  around the model's text.
- **Same grounding check as every card**: an amount, date or id in the draft that is not
  in the transcript sends the note back to the template, with the reason shown.
- **Haiku 4.5 by default** (`CALL_ASSIST_SUMMARY_MODEL`): about 0.4k tokens in and 0.1k
  out per call, under $0.001. A stronger model is a setting, chosen by evidence.
- **A daily spend cap across all model calls** (`CALL_ASSIST_DAILY_BUDGET_USD`, default
  $0.25), estimated at list price from each response's token usage. Once reached,
  understanding runs on rules and the note uses the template until midnight UTC, and the
  console says why. Spend and cap are metrics.

## Consequences
- A useful note at the end of every call, with no customer data sent to the model.
- Every fallback is visible: no key, budget reached, API error, or an unsupported fact.
- The cap is an estimate, not billing: it can drift from the provider's invoice (for
  example on a price change), so the price table in `budget.py` must be kept current.
  Unknown models are priced high, so the cap errs strict.
- Colleague lines now reach the model at call end (ADR 7 sent only caller lines). They
  are spoken words, already heard by the customer; lakehouse data stays out.
