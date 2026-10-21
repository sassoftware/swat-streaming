import logging
from typing import Iterator, List

from confluent_kafka import Consumer, KafkaError, KafkaException, Message, TopicPartition

from .config import KafkaConfig

log = logging.getLogger(__name__)


class KafkaSource:
    def __init__(self, cfg: KafkaConfig):
        self.cfg = cfg
        self._consumer: Consumer | None = None

    def start(self) -> None:
        conf = self.cfg.to_confluent_dict()
        log.info(
            "Starting Kafka consumer",
            extra={
                "bootstrap": self.cfg.bootstrap_servers,
                "topic": self.cfg.topic,
                "group": self.cfg.group_id,
                "security": self.cfg.security_protocol,
            },
        )
        self._consumer = Consumer(conf, logger=log)
        self._consumer.subscribe([self.cfg.topic])

    def poll_batch(self, max_messages: int, timeout_seconds: float) -> List[Message]:
        if self._consumer is None:
            raise RuntimeError("KafkaSource not started")
        msgs = self._consumer.consume(num_messages=max_messages, timeout=timeout_seconds)
        valid: List[Message] = []
        for m in msgs:
            if m.error():
                if m.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(m.error())
            valid.append(m)
        return valid

    def commit(self, messages: List[Message]) -> None:
        if self._consumer is None or not messages:
            return
        offsets = {}
        for m in messages:
            tp = (m.topic(), m.partition())
            if tp not in offsets or m.offset() > offsets[tp]:
                offsets[tp] = m.offset()
        tps = [TopicPartition(t, p, off + 1) for (t, p), off in offsets.items()]
        self._consumer.commit(offsets=tps, asynchronous=False)

    def close(self) -> None:
        if self._consumer is not None:
            try:
                self._consumer.close()
            except Exception:  # noqa: BLE001
                log.warning("Error while closing Kafka consumer", exc_info=True)
            self._consumer = None
