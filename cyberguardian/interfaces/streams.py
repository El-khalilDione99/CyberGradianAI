"""
interfaces/streams.py
─────────────────────
Abstraction du bus d'événements.

  LOCAL  → Kafka / Redpanda  (ENV=local)
  AWS    → Kinesis Data Streams  (ENV=aws)

Aucun code applicatif ne doit importer kafka-python
ou boto3.kinesis directement. Tout passe par ce module.
"""

import json
import os
from abc import ABC, abstractmethod
from typing import Any

ENV = os.getenv("ENV", "local")


# ── Interface commune ────────────────────────────────────────────────────────

class StreamPublisher(ABC):
    @abstractmethod
    def publish(self, stream: str, key: str, payload: dict[str, Any]) -> None:
        pass


class StreamConsumer(ABC):
    @abstractmethod
    def consume(self, stream: str, batch_size: int = 100) -> list[dict[str, Any]]:
        pass


# ── Implémentation locale : Kafka / Redpanda ─────────────────────────────────

class KafkaPublisher(StreamPublisher):
    def __init__(self) -> None:
        from kafka import KafkaProducer  # type: ignore
        bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=3,
        )

    def publish(self, stream: str, key: str, payload: dict[str, Any]) -> None:
        self._producer.send(stream, key=key, value=payload)
        self._producer.flush()


class KafkaConsumer(StreamConsumer):
    """
    Consumer Kafka persistant — une connexion, un poll() non-bloquant.
    Utilise poll(timeout_ms) au lieu de l'itérateur bloquant pour
    éviter le cycle connect/disconnect à chaque appel.
    """

    def __init__(self, group_id: str = "feature-updater", topic: str = "") -> None:
        from kafka import KafkaConsumer as _KafkaConsumer  # type: ignore
        bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self._consumer = _KafkaConsumer(
            bootstrap_servers=bootstrap,
            group_id=group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )
        if topic:
            self._consumer.subscribe([topic])

    def consume(self, stream: str, batch_size: int = 100) -> list[dict[str, Any]]:
        # S'abonner au topic si pas encore fait
        current = self._consumer.subscription() or set()
        if stream not in current:
            self._consumer.subscribe(list(current) + [stream])
        # poll() non-bloquant : retourne immédiatement ce qui est dispo
        records = self._consumer.poll(timeout_ms=2000, max_records=batch_size)
        messages = []
        for partition_records in records.values():
            for msg in partition_records:
                messages.append(msg.value)
        return messages

    def close(self) -> None:
        self._consumer.close()


# ── Implémentation AWS : Kinesis ─────────────────────────────────────────────

class KinesisPublisher(StreamPublisher):
    def __init__(self) -> None:
        import boto3  # type: ignore
        self._client = boto3.client(
            "kinesis",
            region_name=os.getenv("AWS_DEFAULT_REGION", "eu-west-3"),
        )

    def publish(self, stream: str, key: str, payload: dict[str, Any]) -> None:
        self._client.put_record(
            StreamName=stream,
            Data=json.dumps(payload).encode("utf-8"),
            PartitionKey=key,
        )


class KinesisConsumer(StreamConsumer):
    def __init__(self) -> None:
        import boto3  # type: ignore
        self._client = boto3.client(
            "kinesis",
            region_name=os.getenv("AWS_DEFAULT_REGION", "eu-west-3"),
        )

    def consume(self, stream: str, batch_size: int = 100) -> list[dict[str, Any]]:
        response = self._client.describe_stream(StreamName=stream)
        shard_id = response["StreamDescription"]["Shards"][0]["ShardId"]
        iterator_resp = self._client.get_shard_iterator(
            StreamName=stream,
            ShardId=shard_id,
            ShardIteratorType="TRIM_HORIZON",
        )
        records_resp = self._client.get_records(
            ShardIterator=iterator_resp["ShardIterator"], Limit=batch_size
        )
        return [json.loads(r["Data"].decode("utf-8")) for r in records_resp["Records"]]


# ── Factories ────────────────────────────────────────────────────────────────

def get_publisher() -> StreamPublisher:
    if ENV == "aws":
        return KinesisPublisher()
    return KafkaPublisher()


def get_consumer(group_id: str = "feature-updater", topic: str = "") -> StreamConsumer:
    if ENV == "aws":
        return KinesisConsumer()
    return KafkaConsumer(group_id=group_id, topic=topic)
