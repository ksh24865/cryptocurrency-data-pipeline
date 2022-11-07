from contextlib import contextmanager
from json import dumps, loads
from typing import Iterable

from kafka import KafkaConsumer, KafkaProducer


@contextmanager
def kafka_producer_context(
    kafka_host: str = "kafka",
    kafka_port: str = "9092",
) -> Iterable[KafkaProducer]:
    producer = KafkaProducer(
        acks=0,
        compression_type="gzip",
        bootstrap_servers=[f"{kafka_host}:{kafka_port}"],
        value_serializer=lambda x: dumps(x).encode("utf-8"),
    )
    try:
        yield producer
    finally:
        producer.close()


@contextmanager
def kafka_consumer_context(
    topic_name: str,
    kafka_host: str = "kafka",
    kafka_port: str = "9092",
) -> KafkaConsumer:
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=[f"{kafka_host}:{kafka_port}"],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="cryptocurrency",
        value_deserializer=lambda x: loads(x.decode("utf-8")),
    )
    try:
        yield consumer
    finally:
        consumer.close()
