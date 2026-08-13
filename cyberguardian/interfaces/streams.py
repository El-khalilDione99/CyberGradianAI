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
    def __init__(self, group_id: str = "feature-updater") -> None:
        self._group_id = group_id
        self._bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    def consume(self, stream: str, batch_size: int = 100) -> list[dict[str, Any]]:
        from kafka import KafkaConsumer as _KafkaConsumer  # type: ignore
        consumer = _KafkaConsumer(
            stream,
            bootstrap_servers=self._bootstrap,
            group_id=self._group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            consumer_timeout_ms=2000,
        )
        messages = []
        for msg in consumer:
            messages.append(msg.value)
            if len(messages) >= batch_size:
                break
        consumer.close()
        return messages


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


def get_consumer(group_id: str = "feature-updater") -> StreamConsumer:
    if ENV == "aws":
        return KinesisConsumer()
    return KafkaConsumer(group_id=group_id)
