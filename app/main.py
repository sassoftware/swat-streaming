# Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import signal
import sys

from .config import AppConfig
from .logging_setup import configure_logging
from .kafka_consumer import KafkaSource
from .cas_writer import CASWriter
from .message_parser import parse_message, enrich, MessageParseError

log = logging.getLogger(__name__)


class StreamRunner:
    def __init__(self, cfg: AppConfig):
        self.cfg = cfg
        self.kafka = KafkaSource(cfg.kafka)
        self.cas = CASWriter(cfg.cas)
        self._stop = False

    def request_stop(self, *_args) -> None:
        log.info("Shutdown signal received")
        self._stop = True

    def run(self) -> int:
        self.kafka.start()
        self.cas.connect()
        try:
            self._loop()
        finally:
            self.kafka.close()
            self.cas.close()
        return 0

    def _loop(self) -> None:
        batch_size = self.cfg.stream.batch_size
        timeout = self.cfg.stream.batch_timeout_seconds
        fmt = self.cfg.stream.message_format

        while not self._stop:
            messages = self.kafka.poll_batch(max_messages=batch_size, timeout_seconds=timeout)
            if not messages:
                continue

            records = []
            committable = []
            parse_failures = 0
            for m in messages:
                kafka_meta = {
                    "topic": m.topic(),
                    "partition": m.partition(),
                    "offset": m.offset(),
                    "timestamp": m.timestamp()[1] if m.timestamp() else None,
                }
                try:
                    parsed = parse_message(m.value(), fmt)
                except MessageParseError as exc:
                    parse_failures += 1
                    log.warning(
                        "Skipping unparseable message",
                        extra={"error": str(exc), **kafka_meta},
                    )
                    committable.append(m)
                    continue
                records.append(enrich(parsed, kafka_meta))
                committable.append(m)

            if records:
                self.cas.write_batch(records)
            self.kafka.commit(committable)

            log.info(
                "Batch processed",
                extra={
                    "received": len(messages),
                    "written": len(records),
                    "parse_failures": parse_failures,
                },
            )


def main() -> int:
    try:
        cfg = AppConfig.from_env()
    except RuntimeError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2

    configure_logging(cfg.stream.log_level)
    log.info("swat-streaming starting", extra={
        "topic": cfg.kafka.topic,
        "table": f"{cfg.cas.caslib}.{cfg.cas.table_name}",
        "batch_size": cfg.stream.batch_size,
    })

    runner = StreamRunner(cfg)
    signal.signal(signal.SIGTERM, runner.request_stop)
    signal.signal(signal.SIGINT, runner.request_stop)

    try:
        return runner.run()
    except Exception:
        log.exception("Fatal error in stream runner")
        return 1


if __name__ == "__main__":
    sys.exit(main())
