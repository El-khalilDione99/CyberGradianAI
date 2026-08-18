# ── Build stage ─────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements-updater.txt .
RUN pip install --no-cache-dir --user -r requirements-updater.txt

# ── Runtime stage ────────────────────────────────────────────
FROM python:3.11-slim AS runtime

WORKDIR /app

COPY --from=builder /root/.local /root/.local
ENV PATH=/root/.local/bin:$PATH

# Uniquement les modules nécessaires au feature updater
COPY interfaces/  ./interfaces/
COPY engine/      ./engine/

ENV ENV=local
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONPATH=/app

# Variables d'environnement configurables au runtime
ENV KAFKA_BOOTSTRAP_SERVERS=redpanda:9092
ENV REDIS_HOST=redis
ENV REDIS_PORT=6379
ENV FU_BATCH_SIZE=100
ENV FU_POLL_INTERVAL_S=1.0
ENV FU_LOG_EVERY=500

ENTRYPOINT ["python", "-m", "engine.features.main"]
