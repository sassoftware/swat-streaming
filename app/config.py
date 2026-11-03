# Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
from dataclasses import dataclass, field
from typing import Optional


def _env(name: str, default: Optional[str] = None, required: bool = False) -> Optional[str]:
    value = os.environ.get(name, default)
    if required and not value:
        raise RuntimeError(f"Required environment variable {name} is not set")
    return value


def _file_or_env(file_var: str, value_var: str, required: bool = False) -> Optional[str]:
    path = os.environ.get(file_var)
    if path:
        with open(path, "r", encoding="utf-8") as fh:
            return fh.read().strip()
    return _env(value_var, required=required)


@dataclass
class KafkaConfig:
    bootstrap_servers: str
    topic: str
    group_id: str
    security_protocol: str = "SASL_SSL"
    sasl_mechanism: Optional[str] = "SCRAM-SHA-512"
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None
    ssl_ca_location: Optional[str] = None
    ssl_certificate_location: Optional[str] = None
    ssl_key_location: Optional[str] = None
    auto_offset_reset: str = "latest"
    session_timeout_ms: int = 45000
    max_poll_interval_ms: int = 300000

    @classmethod
    def from_env(cls) -> "KafkaConfig":
        return cls(
            bootstrap_servers=_env("KAFKA_BOOTSTRAP_SERVERS", required=True),
            topic=_env("KAFKA_TOPIC", required=True),
            group_id=_env("KAFKA_GROUP_ID", required=True),
            security_protocol=_env("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
            sasl_mechanism=_env("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512"),
            sasl_username=_file_or_env("KAFKA_SASL_USERNAME_FILE", "KAFKA_SASL_USERNAME"),
            sasl_password=_file_or_env("KAFKA_SASL_PASSWORD_FILE", "KAFKA_SASL_PASSWORD"),
            ssl_ca_location=_env("KAFKA_SSL_CA_LOCATION"),
            ssl_certificate_location=_env("KAFKA_SSL_CERTIFICATE_LOCATION"),
            ssl_key_location=_env("KAFKA_SSL_KEY_LOCATION"),
            auto_offset_reset=_env("KAFKA_AUTO_OFFSET_RESET", "latest"),
            session_timeout_ms=int(_env("KAFKA_SESSION_TIMEOUT_MS", "45000")),
            max_poll_interval_ms=int(_env("KAFKA_MAX_POLL_INTERVAL_MS", "300000")),
        )

    def to_confluent_dict(self) -> dict:
        cfg = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": self.auto_offset_reset,
            "session.timeout.ms": self.session_timeout_ms,
            "max.poll.interval.ms": self.max_poll_interval_ms,
            "security.protocol": self.security_protocol,
        }
        if self.security_protocol in ("SASL_SSL", "SASL_PLAINTEXT"):
            cfg["sasl.mechanism"] = self.sasl_mechanism
            cfg["sasl.username"] = self.sasl_username
            cfg["sasl.password"] = self.sasl_password
        if self.security_protocol in ("SSL", "SASL_SSL"):
            if self.ssl_ca_location:
                cfg["ssl.ca.location"] = self.ssl_ca_location
            if self.ssl_certificate_location:
                cfg["ssl.certificate.location"] = self.ssl_certificate_location
            if self.ssl_key_location:
                cfg["ssl.key.location"] = self.ssl_key_location
        return cfg


@dataclass
class CASConfig:
    hostname: str
    port: Optional[int] = None
    protocol: str = "https"
    username: Optional[str] = None
    password: Optional[str] = None
    token: Optional[str] = None
    ssl_ca_list: Optional[str] = None
    caslib: str = "CASUSER"
    table_name: str = "kafka_stream"
    promote: bool = True
    persist_on_shutdown: bool = False
    persist_format: str = "parquet"

    @classmethod
    def from_env(cls) -> "CASConfig":
        port_raw = _env("CAS_PORT")
        return cls(
            hostname=_env("CAS_HOST", required=True),
            port=int(port_raw) if port_raw else None,
            protocol=_env("CAS_PROTOCOL", "https"),
            username=_file_or_env("CAS_USERNAME_FILE", "CAS_USERNAME"),
            password=_file_or_env("CAS_PASSWORD_FILE", "CAS_PASSWORD"),
            token=_file_or_env("CAS_TOKEN_FILE", "CAS_TOKEN"),
            ssl_ca_list=_env("CAS_SSL_CA_LIST"),
            caslib=_env("CAS_CASLIB", "CASUSER"),
            table_name=_env("CAS_TABLE_NAME", "kafka_stream"),
            promote=_env("CAS_TABLE_PROMOTE", "true").lower() == "true",
            persist_on_shutdown=_env("CAS_PERSIST_ON_SHUTDOWN", "false").lower() == "true",
            persist_format=_env("CAS_PERSIST_FORMAT", "parquet"),
        )


@dataclass
class StreamConfig:
    batch_size: int = 500
    batch_timeout_seconds: float = 5.0
    message_format: str = "json"
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "StreamConfig":
        return cls(
            batch_size=int(_env("BATCH_SIZE", "500")),
            batch_timeout_seconds=float(_env("BATCH_TIMEOUT_SECONDS", "5")),
            message_format=_env("MESSAGE_FORMAT", "json").lower(),
            log_level=_env("LOG_LEVEL", "INFO").upper(),
        )


@dataclass
class AppConfig:
    kafka: KafkaConfig = field(default_factory=KafkaConfig.from_env)
    cas: CASConfig = field(default_factory=CASConfig.from_env)
    stream: StreamConfig = field(default_factory=StreamConfig.from_env)

    @classmethod
    def from_env(cls) -> "AppConfig":
        return cls(
            kafka=KafkaConfig.from_env(),
            cas=CASConfig.from_env(),
            stream=StreamConfig.from_env(),
        )
