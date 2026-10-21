import os
from dataclasses import dataclass
from typing import Optional

from app.config import KafkaConfig, _env


@dataclass
class ProducerConfig:
    rate_per_second: float = 50.0
    duration_seconds: Optional[float] = None
    total_messages: Optional[int] = None
    num_accounts: int = 5_000
    num_merchants: int = 800
    fraud_rate: float = 0.01
    seed: Optional[int] = None
    log_level: str = "INFO"
    flush_every: int = 500

    @classmethod
    def from_env(cls) -> "ProducerConfig":
        dur = os.environ.get("PRODUCER_DURATION_SECONDS")
        total = os.environ.get("PRODUCER_TOTAL_MESSAGES")
        seed = os.environ.get("PRODUCER_SEED")
        return cls(
            rate_per_second=float(_env("PRODUCER_RATE_PER_SECOND", "50")),
            duration_seconds=float(dur) if dur else None,
            total_messages=int(total) if total else None,
            num_accounts=int(_env("PRODUCER_NUM_ACCOUNTS", "5000")),
            num_merchants=int(_env("PRODUCER_NUM_MERCHANTS", "800")),
            fraud_rate=float(_env("PRODUCER_FRAUD_RATE", "0.01")),
            seed=int(seed) if seed else None,
            log_level=_env("LOG_LEVEL", "INFO").upper(),
            flush_every=int(_env("PRODUCER_FLUSH_EVERY", "500")),
        )


def kafka_producer_dict(kafka: KafkaConfig) -> dict:
    cfg = {
        "bootstrap.servers": kafka.bootstrap_servers,
        "security.protocol": kafka.security_protocol,
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "zstd",
        "linger.ms": 20,
        "batch.size": 64 * 1024,
        "message.timeout.ms": 60000,
    }
    debug = os.environ.get("KAFKA_DEBUG")
    if debug:
        cfg["debug"] = debug
    if kafka.security_protocol in ("SASL_SSL", "SASL_PLAINTEXT"):
        cfg["sasl.mechanism"] = kafka.sasl_mechanism
        cfg["sasl.username"] = kafka.sasl_username
        cfg["sasl.password"] = kafka.sasl_password
    if kafka.security_protocol in ("SSL", "SASL_SSL"):
        if kafka.ssl_ca_location:
            cfg["ssl.ca.location"] = kafka.ssl_ca_location
        if kafka.ssl_certificate_location:
            cfg["ssl.certificate.location"] = kafka.ssl_certificate_location
        if kafka.ssl_key_location:
            cfg["ssl.key.location"] = kafka.ssl_key_location
    return cfg
