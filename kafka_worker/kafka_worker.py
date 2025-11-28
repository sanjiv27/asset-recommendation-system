import json
import os
import signal
import threading
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_EXCEPTION
from typing import Callable, Dict, List, Optional

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

import processing
import db
import recommendation_engine


Processor = Callable[[dict], None]


REGISTRY: Dict[str, Processor] = {
    "userprofile": processing.process_userprofile,
    "retraining": processing.process_retraining,
    "interactions": processing.process_interactions,
}


_shutdown = threading.Event()

engine = recommendation_engine.RecommendationEngine()


def _get_env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value not in (None, "") else default


def _producer(broker: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else None,
        linger_ms=5,
        retries=3,
    )


def _make_consumer(broker: str, group_id: str, topics: List[str]) -> KafkaConsumer:
    consumer = KafkaConsumer(
        bootstrap_servers=broker,
        group_id=group_id,
        enable_auto_commit=True,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: v.decode("utf-8"),
        key_deserializer=lambda v: v.decode("utf-8") if v else None,
        consumer_timeout_ms=1000,
        max_poll_records=100,
    )
    consumer.subscribe(topics)
    return consumer


def _handle_message(
    topic: str,
    raw_value: str,
    max_retries: int,
    dlq_topic: Optional[str],
    producer: Optional[KafkaProducer],
) -> None:
    processor = REGISTRY.get(topic)
    if processor is None:
        return

    try:
        payload = json.loads(raw_value)
    except json.JSONDecodeError:
        _send_dlq(producer, dlq_topic, topic, raw_value, "json_decode_error")
        return

    attempt = 0
    while True:
        try:
            processor(payload)
            return
        except Exception as e:
            attempt += 1
            if attempt > max_retries:
                _send_dlq(producer, dlq_topic, topic, payload, f"processing_error: {e}")
                return


def _send_dlq(
    producer: Optional[KafkaProducer],
    dlq_topic: Optional[str],
    source_topic: str,
    payload: object,
    error: str,
) -> None:
    if not producer or not dlq_topic:
        return
    try:
        producer.send(
            dlq_topic,
            {
                "source_topic": source_topic,
                "error": error,
                "payload": payload,
            },
        )
        producer.flush(1.0)
    except KafkaError:
        pass


def _install_signal_handlers() -> None:
    def _signal_handler(_signum, _frame):
        _shutdown.set()

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)


def run_worker() -> None:
    broker = _get_env("KAFKA_BROKER_URL", "kafka:9092")
    group_id = _get_env("KAFKA_GROUP_ID", "asset-reco-workers")
    topics = [t.strip() for t in _get_env("KAFKA_TOPICS", "userprofile,retraining,interactions").split(",") if t.strip()]
    dlq_topic = os.getenv("KAFKA_DLQ_TOPIC")
    max_retries = int(_get_env("WORKER_MAX_RETRIES", "3"))
    concurrency = int(_get_env("WORKER_CONCURRENCY", "4"))

    consumer = _make_consumer(broker, group_id, topics)
    producer = _producer(broker) if dlq_topic else None

    _install_signal_handlers()

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = set()
        while not _shutdown.is_set():
            try:
                for msg in consumer:
                    if _shutdown.is_set():
                        break
                    future = pool.submit(
                        _handle_message,
                        msg.topic,
                        msg.value,
                        max_retries,
                        dlq_topic,
                        producer,
                    )
                    futures.add(future)
                    done, futures = wait(futures, timeout=0, return_when=FIRST_EXCEPTION)
                    for d in list(done):
                        if d.exception():
                            pass
            except Exception:
                continue

    try:
        consumer.close()
    except Exception:
        pass
    try:
        if producer:
            producer.flush(1.0)
            producer.close()
    except Exception:
        pass
    # Flush any remaining interactions to database
    try:
        db.flush_interactions_buffer()
    except Exception:
        pass


if __name__ == "__main__":
    run_worker()


