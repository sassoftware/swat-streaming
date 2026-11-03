# Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import signal
import sys
import time
from typing import Optional

from confluent_kafka import Producer

from app.config import KafkaConfig
from app.logging_setup import configure_logging
from .config import ProducerConfig, kafka_producer_dict
from .generator import TransactionGenerator

log = logging.getLogger(__name__)


class StopFlag:
    def __init__(self):
        self.stop = False

    def set(self, *_args):
        log.info("Producer shutdown signal received")
        self.stop = True


def _on_delivery(err, msg) -> None:
    if err is not None:
        log.error("Delivery failed", extra={"error": str(err), "topic": msg.topic()})


def _on_error(err) -> None:
    # librdkafka surfaces broker-level errors (auth failures, unknown topic,
    # TLS handshake problems, …) via this callback. Without it those errors
    # are invisible and the only symptom is per-message delivery timeouts.
    log.error(
        "Kafka client error",
        extra={
            "code": err.code(),
            "name": err.name(),
            "fatal": err.fatal(),
            "retriable": err.retriable(),
            "reason": err.str(),
        },
    )


def _should_continue(stop: StopFlag, start: float, sent: int, cfg: ProducerConfig) -> bool:
    if stop.stop:
        return False
    if cfg.total_messages is not None and sent >= cfg.total_messages:
        return False
    if cfg.duration_seconds is not None and (time.monotonic() - start) >= cfg.duration_seconds:
        return False
    return True


def run() -> int:
    try:
        kafka_cfg = KafkaConfig.from_env()
        prod_cfg = ProducerConfig.from_env()
    except RuntimeError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2

    configure_logging(prod_cfg.log_level)
    log.info(
        "Producer starting",
        extra={
            "topic": kafka_cfg.topic,
            "rate": prod_cfg.rate_per_second,
            "duration_s": prod_cfg.duration_seconds,
            "total": prod_cfg.total_messages,
            "fraud_rate": prod_cfg.fraud_rate,
        },
    )

    producer = Producer(
        kafka_producer_dict(kafka_cfg),
        logger=log,
        error_cb=_on_error,
    )
    generator = TransactionGenerator(
        num_accounts=prod_cfg.num_accounts,
        num_merchants=prod_cfg.num_merchants,
        fraud_rate=prod_cfg.fraud_rate,
        seed=prod_cfg.seed,
    )

    stop = StopFlag()
    signal.signal(signal.SIGTERM, stop.set)
    signal.signal(signal.SIGINT, stop.set)

    interval = 1.0 / prod_cfg.rate_per_second if prod_cfg.rate_per_second > 0 else 0.0
    start = time.monotonic()
    next_send = start
    sent = 0
    fraud_sent = 0

    while _should_continue(stop, start, sent, prod_cfg):
        now = time.monotonic()
        if now < next_send:
            time.sleep(min(0.05, next_send - now))
            continue

        txn = generator.next()
        payload = json.dumps(txn.as_dict()).encode("utf-8")
        producer.produce(
            topic=kafka_cfg.topic,
            key=txn.account_id.encode("utf-8"),
            value=payload,
            on_delivery=_on_delivery,
        )
        sent += 1
        if txn.is_fraud:
            fraud_sent += 1
        next_send += interval

        if sent % prod_cfg.flush_every == 0:
            producer.poll(0)
            elapsed = time.monotonic() - start
            log.info(
                "Producer progress",
                extra={
                    "sent": sent,
                    "fraud_sent": fraud_sent,
                    "elapsed_s": round(elapsed, 1),
                    "actual_rate": round(sent / elapsed, 1) if elapsed > 0 else None,
                },
            )

    log.info("Flushing remaining messages")
    producer.flush(timeout=30)
    log.info(
        "Producer finished",
        extra={"sent": sent, "fraud_sent": fraud_sent,
               "elapsed_s": round(time.monotonic() - start, 1)},
    )
    return 0


if __name__ == "__main__":
    sys.exit(run())
