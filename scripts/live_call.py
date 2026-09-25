# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx>=0.28"]
# ///
"""Run a Live Call Assist demo call headlessly and print what the colleague would see.

Usage: uv run scripts/live_call.py <colleague> <scenario> [--json] [--speed=3]
Exit code 0 if the call completed with at least one card, 1 otherwise.
The password grant is a local-demo shortcut; the real console uses SSO + PKCE.
"""

from __future__ import annotations

import json
import pathlib
import sys
import time

import httpx

ROOT = pathlib.Path(__file__).resolve().parent.parent
ENV = dict(
    line.split("=", 1)
    for line in (ROOT / ".env").read_text().splitlines()
    if "=" in line
)
BASE = "http://localhost:8090"
COLOUR = {
    "guard": 31,
    "compliance": 33,
    "action": 32,
    "identity": 35,
    "insight": 36,
    "status": 90,
    "summary": 34,
}


def token(user: str) -> str:
    r = httpx.post(
        "http://localhost:8280/realms/bank/protocol/openid-connect/token",
        data={
            "grant_type": "password",
            "client_id": "agent-console",
            "username": user,
            "password": ENV["DEMO_USER_PASSWORD"],
        },
        timeout=10,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def main(user: str, scenario: str, as_json: bool, speed: float | None = None) -> int:
    headers = {"Authorization": f"Bearer {token(user)}"}
    started = time.monotonic()
    r = httpx.post(
        f"{BASE}/api/calls", json={"scenario": scenario, "speed": speed}, headers=headers, timeout=10
    )
    r.raise_for_status()
    call = r.json()
    if not as_json:
        print(f"▶ {call['call_id']} · {call['title']} · colleague {user}\n")
    cards, events = [], []
    with httpx.stream(
        "GET",
        f"{BASE}/api/calls/{call['call_id']}/events",
        headers=headers,
        timeout=None,
    ) as s:
        for line in s.iter_lines():
            if not line.startswith("data: "):
                continue
            ev = json.loads(line[6:])
            events.append(ev)
            t = ev["type"]
            if t == "transcript" and not as_json:
                who = "CALLER " if ev["speaker"] == "customer" else "YOU    "
                print(f"  {who}│ {ev['text']}")
            elif t == "tool_call" and not as_json:
                print(f"         ↳ {ev['tool']} {ev['outcome']} {ev.get('ms', '')}ms")
            elif t == "card":
                c = ev["card"]
                cards.append(c)
                if not as_json:
                    lock = " 🔒verify-first" if c["requires_verification"] else ""
                    proc = f" [{c['procedure']['id']}]" if c.get("procedure") else ""
                    print(
                        f"\033[{COLOUR[c['kind']]}m  ◆ {c['kind'].upper()}: {c['title']}{proc}{lock} ({c['latency_ms']} ms)\033[0m"
                    )
                    print(f"      {c['body']}")
            elif t == "withheld" and not as_json:
                print(
                    f"\033[31m  ✖ withheld ungrounded card: {ev['title']} {ev['unsupported']}\033[0m"
                )
            elif t == "ended":
                break
    if as_json:
        print(json.dumps({"call": call, "cards": cards, "events": events}, default=str))
    else:
        print(
            f"\n■ call ended after {time.monotonic() - started:.0f}s · {len(cards)} cards"
        )
    return 0 if cards else 1


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    speed = next((float(a.split("=", 1)[1]) for a in sys.argv if a.startswith("--speed=")), None)
    sys.exit(main(args[0], args[1], "--json" in sys.argv, speed))
