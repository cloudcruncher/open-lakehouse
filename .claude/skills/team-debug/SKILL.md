---
name: team-debug
description: Spawn an agent team to debug with competing hypotheses, where investigators try to disprove each other's theories.
argument-hint: "<symptom, and any hypotheses you already have>"
disable-model-invocation: true
---
Create an agent team (not subagents) to find the root cause of: $ARGUMENTS

First, read the symptom and write 3 to 5 distinct hypotheses, adding any the user gave. Make them genuinely different (for example data, config, ordering/timing, permissions), not variations of one idea.

Spawn one teammate per hypothesis, agent type `hypothesis-investigator`, named `h1`, `h2`, and so on. Each spawn prompt must contain: the symptom verbatim, the hypothesis it owns, the relevant services and log locations you already know, and the instruction to message the other investigators by name and try to disprove their theories.

Rules for you as lead:
- Do not investigate yourself. Wait for all investigators to report.
- Teammates are read-only. Any restart, data change or fix is your call and needs the user's approval first.
- Finish with: the surviving theory and its evidence, the theories disproved and why, and the smallest fix to try. If nothing survived, say so and propose new hypotheses instead of forcing a conclusion.
- Shut the teammates down afterwards.
