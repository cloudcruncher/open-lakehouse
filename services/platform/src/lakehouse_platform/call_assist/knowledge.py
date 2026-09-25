"""Procedure retrieval: BM25 over the bank's procedure library, with citations.

Guidance cards cite a procedure id, so a colleague (and a reviewer) can check where
advice came from. Lexical BM25 is the right first retriever for short, controlled,
jargon-heavy procedures: exact terms like "DISP" or "chargeback" matter more than
semantic similarity. At scale this becomes hybrid retrieval (BM25 + embeddings,
reranked) behind the same `search()` interface. The engine doesn't change.
"""

from __future__ import annotations

import math
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

TOKEN = re.compile(r"[a-z0-9_]+")
STOP = frozenset(
    "a an the and or of to in on for with is are be it this that as at by from not no do don't if "
    "we you your our they them their i my me".split()
)


def tokens(text: str) -> list[str]:
    return [t for t in TOKEN.findall(text.lower()) if t not in STOP]


@dataclass(frozen=True)
class Procedure:
    id: str
    title: str
    tags: str
    body: str

    def excerpt(self, n: int = 2) -> str:
        steps = [ln.strip() for ln in self.body.splitlines() if ln.strip()]
        return " ".join(steps[:n])


def load(directory: Path) -> list[Procedure]:
    procs = []
    for path in sorted(directory.glob("*.md")):
        text = path.read_text()
        if not text.startswith("---"):
            continue
        _, front, body = text.split("---", 2)
        meta = dict(line.split(":", 1) for line in front.strip().splitlines())
        procs.append(
            Procedure(meta["id"].strip(), meta["title"].strip(), meta.get("tags", "").strip(), body.strip())
        )
    return procs


class ProcedureIndex:
    def __init__(self, procedures: list[Procedure], k1: float = 1.4, b: float = 0.75) -> None:
        self.procs = procedures
        self.k1, self.b = k1, b
        # Title and tags are weighted by repetition: they describe what the procedure is *for*.
        self.docs = [Counter(tokens(f"{p.title} {p.title} {p.tags} {p.tags} {p.body}")) for p in procedures]
        self.lengths = [sum(d.values()) for d in self.docs]
        self.avg = sum(self.lengths) / max(1, len(self.lengths))
        df: Counter[str] = Counter()
        for d in self.docs:
            df.update(d.keys())
        n = len(self.docs)
        self.idf = {t: math.log(1 + (n - f + 0.5) / (f + 0.5)) for t, f in df.items()}

    @classmethod
    def default(cls) -> ProcedureIndex:
        return cls(load(Path(__file__).parent / "knowledge"))

    def by_id(self, proc_id: str) -> Procedure | None:
        return next((p for p in self.procs if p.id == proc_id), None)

    def search(self, query: str, k: int = 1) -> list[tuple[Procedure, float]]:
        q = tokens(query)
        scored = []
        for p, d, length in zip(self.procs, self.docs, self.lengths, strict=True):
            score = 0.0
            for t in q:
                f = d.get(t, 0)
                if f:
                    norm = f * (self.k1 + 1) / (f + self.k1 * (1 - self.b + self.b * length / self.avg))
                    score += self.idf[t] * norm
            if score > 0:
                scored.append((p, score))
        return sorted(scored, key=lambda x: -x[1])[:k]
