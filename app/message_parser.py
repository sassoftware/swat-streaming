import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict

log = logging.getLogger(__name__)


class MessageParseError(Exception):
    pass


def parse_message(raw_value: bytes, fmt: str) -> Dict[str, Any]:
    if raw_value is None:
        raise MessageParseError("Empty message payload")

    if fmt == "json":
        try:
            decoded = json.loads(raw_value.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise MessageParseError(f"Invalid JSON: {exc}") from exc
        if not isinstance(decoded, dict):
            raise MessageParseError("Top-level JSON must be an object")
        return decoded

    if fmt == "raw":
        return {"value": raw_value.decode("utf-8", errors="replace")}

    raise MessageParseError(f"Unsupported message format: {fmt}")


def enrich(record: Dict[str, Any], kafka_meta: Dict[str, Any]) -> Dict[str, Any]:
    record.setdefault("_kafka_topic", kafka_meta["topic"])
    record.setdefault("_kafka_partition", kafka_meta["partition"])
    record.setdefault("_kafka_offset", kafka_meta["offset"])
    record.setdefault("_kafka_timestamp", kafka_meta["timestamp"])
    record.setdefault("_ingested_at", datetime.now(timezone.utc).isoformat())
    return record
