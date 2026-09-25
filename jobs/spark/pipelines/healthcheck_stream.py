"""Container healthcheck for the CDC stream: healthy only if a micro-batch finished recently.

A stream can be "running" yet stuck (a hung commit, a dead Kafka connection that
never errors). Liveness here means progress, so the healer restarts a stalled
stream, and Spark resumes from its checkpoint.
"""

import os
import sys
import time
import urllib.request

MAX_SILENCE_S = float(os.environ.get("CDC_MAX_SILENCE_S", 120))

try:
    body = urllib.request.urlopen("http://localhost:9108/metrics", timeout=3).read().decode()
except OSError:
    sys.exit(1)
for line in body.splitlines():
    if line.startswith("cdc_last_progress_timestamp_seconds "):
        last = float(line.split()[1])
        sys.exit(0 if last and time.time() - last < MAX_SILENCE_S else 1)
sys.exit(1)
