"""
engine/datasets/_io.py
──────────────────────
Helpers de sérialisation Parquet ↔ bytes pour l'ObjectStore.

L'ObjectStore (interfaces/store.py) sait déjà lire/écrire du JSON et des
modèles picklés. On lui ajoute ici le Parquet, format retenu pour les
matrices de features (colonnaire, compact, natif SageMaker / Athena).
"""

from __future__ import annotations

import io
import math
import subprocess
from typing import Any

import pandas as pd


def df_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    return buf.getvalue()


def parquet_bytes_to_df(data: bytes) -> pd.DataFrame:
    return pd.read_parquet(io.BytesIO(data), engine="pyarrow")


def save_parquet(store, bucket: str, key: str, df: pd.DataFrame) -> None:
    store.upload(bucket, key, df_to_parquet_bytes(df))


def load_parquet(store, bucket: str, key: str) -> pd.DataFrame:
    return parquet_bytes_to_df(store.download(bucket, key))


def records_from_df(df: pd.DataFrame) -> list[dict[str, Any]]:
    """
    df.to_dict("records") en remplaçant les NaN pandas par None
    (les événements bruts contiennent des champs optionnels : id_scenario…).
    """
    out: list[dict[str, Any]] = []
    for rec in df.to_dict("records"):
        out.append({
            k: (None if isinstance(v, float) and math.isnan(v) else v)
            for k, v in rec.items()
        })
    return out


def git_sha() -> str:
    """SHA court du commit courant (best-effort — 'unknown' si indisponible)."""
    try:
        r = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=5,
        )
        return r.stdout.strip() or "unknown"
    except Exception:
        return "unknown"
