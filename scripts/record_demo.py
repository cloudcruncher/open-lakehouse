# /// script
# requires-python = ">=3.12"
# dependencies = ["playwright>=1.55", "pillow>=11"]
# ///
"""Record the Live Call Assist console handling a live call, as an animated GIF.

Signs in through Keycloak (SSO + PKCE, like a colleague), starts a demo call,
clicks "ID&V confirmed" once identity is shown, captures frames until the call ends,
then opens a card's platform x-ray (how the lakehouse produced the answer).
Needs: `uv run --with playwright python -m playwright install chromium` once.

Usage: uv run scripts/record_demo.py [scenario] [--out docs/img/live-call-assist.gif]
"""

from __future__ import annotations

import io
import sys
import time
from pathlib import Path

from PIL import Image
from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parent.parent
ENV = dict(
    line.split("=", 1)
    for line in (ROOT / ".env").read_text().splitlines()
    if "=" in line
)
SCENARIO = next((a for a in sys.argv[1:] if not a.startswith("--")), "card_fraud")
OUT = Path(
    next(
        (a.split("=", 1)[1] for a in sys.argv if a.startswith("--out=")),
        "docs/img/live-call-assist.gif",
    )
)
TITLES = {
    "card_fraud": "Card stolen",
    "complaint_chase": "Chasing a complaint",
    "payment_missing": "Faster payment",
    "bereavement": "Bereavement",
    "injection": "manipulate",
}


def main() -> None:
    frames: list[Image.Image] = []
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(
            viewport={"width": 1440, "height": 860}, device_scale_factor=1
        )
        page.goto("http://localhost:8090/")
        page.click("#login")
        page.fill("#username", "alice")
        page.fill("#password", ENV["DEMO_USER_PASSWORD"])
        page.click("#kc-login")
        page.wait_for_selector(".scenarios button")

        def snap(hold: int = 1) -> None:
            img = Image.open(io.BytesIO(page.screenshot())).convert("RGB")
            img = img.resize((1080, 645), Image.LANCZOS)
            frames.extend([img] * hold)

        snap(3)
        page.get_by_role("button", name=TITLES[SCENARIO], exact=False).first.click()
        verified = False
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            time.sleep(0.8)
            snap()
            if not verified and page.is_visible("#verifybar"):
                time.sleep(2.5)
                snap(2)
                page.click("#verify")
                verified = True
            if page.locator(".card.summary").count():
                break
        # The draft call note lands when the call ends; hold on it.
        page.wait_for_selector(".card.summary", timeout=30000)
        time.sleep(0.5)
        snap(4)
        # End on the platform, not just the assistant: open the provenance chain of the
        # card that found the payment (snapshot, Trino, OPA decision, audit row).
        xray = page.locator(".card:has(.xray)").first.locator(".xray")
        if xray.count():
            xray.evaluate("d => d.open = true")
            xray.scroll_into_view_if_needed()
            time.sleep(0.4)
        snap(10)
        browser.close()

    OUT.parent.mkdir(parents=True, exist_ok=True)
    palette = [f.quantize(colors=128, method=Image.Quantize.MEDIANCUT) for f in frames]
    palette[0].save(
        OUT,
        save_all=True,
        append_images=palette[1:],
        duration=700,
        loop=0,
        optimize=True,
    )
    print(f"wrote {OUT} ({len(frames)} frames, {OUT.stat().st_size / 1e6:.1f} MB)")


if __name__ == "__main__":
    main()
