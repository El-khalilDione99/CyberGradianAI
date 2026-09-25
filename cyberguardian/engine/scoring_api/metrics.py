"""
engine/scoring_api/metrics.py
──────────────────────────────
Métriques du service de scoring (contrat v1 §5 et §7).

  METRICS_BACKEND=local       → en mémoire, lisibles sur GET /metrics
  METRICS_BACKEND=cloudwatch  → en plus, publication toutes les
                                METRICS_FLUSH_S secondes dans l'espace de noms
                                CyberGuardian/Scoring (latence p50/p99,
                                volume, taux d'alerte, distribution des scores)
"""

from __future__ import annotations

import logging
import os
import threading
from collections import Counter, deque
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)

BACKEND   = os.getenv("METRICS_BACKEND", "local").lower()
NAMESPACE = os.getenv("METRICS_NAMESPACE", "CyberGuardian/Scoring")
FLUSH_S   = float(os.getenv("METRICS_FLUSH_S", "60"))


class Metrics:
    """Collecte thread-safe ; fenêtre glissante des 10 000 dernières latences."""

    def __init__(self, backend: str = BACKEND) -> None:
        self.backend = backend
        self._lock = threading.Lock()
        self._latences: deque[float] = deque(maxlen=10_000)
        self._decisions: Counter[str] = Counter()
        self._tranches: Counter[int] = Counter()            # tranche de 10 points de score
        self._total = 0
        self._a_publier: list[tuple[float, int, str]] = []  # (latence, score, décision) depuis la dernière publication
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def enregistrer(self, latence_ms: float, score: int, decision: str) -> None:
        with self._lock:
            self._total += 1
            self._latences.append(latence_ms)
            self._decisions[decision] += 1
            self._tranches[min(score // 10, 9)] += 1
            if self.backend == "cloudwatch":
                self._a_publier.append((latence_ms, score, decision))

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            lat = np.array(self._latences) if self._latences else np.array([0.0])
            n = max(self._total, 1)
            return {
                "backend": self.backend,
                "requetes": self._total,
                "latence_ms": {"p50": round(float(np.percentile(lat, 50)), 2),
                               "p99": round(float(np.percentile(lat, 99)), 2)},
                "decisions": {d: {"nombre": self._decisions[d], "taux": round(self._decisions[d] / n, 4)}
                              for d in ("PASS", "CHALLENGE", "BLOCK")},
                "taux_alerte": round(self._decisions["BLOCK"] / n, 4),
                "distribution_scores": {f"{10 * t}-{10 * t + 9}": self._tranches[t] for t in range(10)},
            }

    # ── Publication CloudWatch ────────────────────────────────

    def demarrer(self) -> None:
        if self.backend != "cloudwatch" or self._thread:
            return
        self._thread = threading.Thread(target=self._boucle, name="metrics-cloudwatch", daemon=True)
        self._thread.start()

    def arreter(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)
            self.publier()

    def _boucle(self) -> None:
        while not self._stop.wait(FLUSH_S):
            self.publier()

    def publier(self) -> None:
        with self._lock:
            lot, self._a_publier = self._a_publier, []
        if not lot:
            return
        lat = np.array([x[0] for x in lot]); scores = Counter(x[1] for x in lot)
        n = len(lot); decisions = Counter(x[2] for x in lot)
        donnees = [
            {"MetricName": "LatenceP50", "Value": float(np.percentile(lat, 50)), "Unit": "Milliseconds"},
            {"MetricName": "LatenceP99", "Value": float(np.percentile(lat, 99)), "Unit": "Milliseconds"},
            {"MetricName": "Requetes", "Value": n, "Unit": "Count"},
            {"MetricName": "TauxAlerte", "Value": 100 * decisions["BLOCK"] / n, "Unit": "Percent"},
            {"MetricName": "TauxChallenge", "Value": 100 * decisions["CHALLENGE"] / n, "Unit": "Percent"},
            {"MetricName": "ScoreFinal", "Values": [float(v) for v in scores][:150],
             "Counts": [float(c) for c in scores.values()][:150], "Unit": "None"},
        ]
        try:
            import boto3  # type: ignore
            boto3.client("cloudwatch", region_name=os.getenv("AWS_DEFAULT_REGION", "eu-west-3")).put_metric_data(
                Namespace=NAMESPACE, MetricData=donnees)
        except Exception as exc:
            logger.error("Publication CloudWatch échouée (%d points perdus) : %s", n, exc)
