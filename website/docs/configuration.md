---
sidebar_position: 4
title: Configuration reference
description: Every environment variable read by the swat-streaming consumer and the synthetic producer.
---

# Configuration reference

All configuration comes from environment variables. In Kubernetes, non-sensitive values come from the `swat-streaming-config` ConfigMap, and credentials come from files mounted from Secrets.

:::info File-based secrets
Every credential has a `*_FILE` variant. When `X_FILE` is set, the app reads the value from that file (with surrounding whitespace stripped) and **ignores** the plain `X` variable. File-based secrets are recommended because, unlike environment variables, they don't show up in `kubectl describe pod` or in process listings.
:::

Boolean variables are `true` only when their value is the string `true` (case-insensitive). Any other value counts as `false`.

## Kafka

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | *required* | Comma-separated `host:port` broker list |
| `KAFKA_TOPIC` | *required* | Topic to consume (or, for the producer, to produce to) |
| `KAFKA_GROUP_ID` | *required* | Consumer group ID. Also required by the producer, which ignores it |
| `KAFKA_SECURITY_PROTOCOL` | `SASL_SSL` | `SASL_SSL`, `SSL`, `SASL_PLAINTEXT`, or `PLAINTEXT` |
| `KAFKA_SASL_MECHANISM` | `SCRAM-SHA-512` | `SCRAM-SHA-512`, `SCRAM-SHA-256`, or `PLAIN`. Used only with `SASL_*` protocols |
| `KAFKA_SASL_USERNAME` / `KAFKA_SASL_USERNAME_FILE` | – | SASL username |
| `KAFKA_SASL_PASSWORD` / `KAFKA_SASL_PASSWORD_FILE` | – | SASL password |
| `KAFKA_SSL_CA_LOCATION` | – | Path to the CA certificate that signed the broker certificates. Used with `SSL` / `SASL_SSL` |
| `KAFKA_SSL_CERTIFICATE_LOCATION` | – | Client certificate path, for mTLS |
| `KAFKA_SSL_KEY_LOCATION` | – | Client private key path, for mTLS |
| `KAFKA_AUTO_OFFSET_RESET` | `latest` | Where a new consumer group starts: `latest` or `earliest` |
| `KAFKA_SESSION_TIMEOUT_MS` | `45000` | Consumer group session timeout |
| `KAFKA_MAX_POLL_INTERVAL_MS` | `300000` | Maximum time between polls before the consumer is evicted from the group. Must be longer than your slowest CAS write, including retries |

Auto-commit is always disabled. Offsets are committed by the app after each successful CAS write.

## CAS

| Variable | Default | Description |
|---|---|---|
| `CAS_HOST` | *required* | For `https`: the full URL of the CAS REST endpoint, for example `https://viya.example.com/cas-shared-default-http`. For `cas`: the controller hostname |
| `CAS_PORT` | – | CAS port. Usually unset for REST URLs; for example `5570` for the binary protocol |
| `CAS_PROTOCOL` | `https` | `https` / `http` (REST) or `cas` (binary) |
| `CAS_TOKEN` / `CAS_TOKEN_FILE` | – | OAuth access token. When set, it is used instead of username/password |
| `CAS_USERNAME` / `CAS_USERNAME_FILE` | – | CAS username (used only when no token is set) |
| `CAS_PASSWORD` / `CAS_PASSWORD_FILE` | – | CAS password (used only when no token is set) |
| `CAS_SSL_CA_LIST` | – | Path to the CA bundle used to verify the Viya TLS certificate |
| `CAS_CASLIB` | `CASUSER` | Caslib that holds the target table. Must already exist |
| `CAS_TABLE_NAME` | `kafka_stream` | Name of the in-memory target table |
| `CAS_TABLE_PROMOTE` | `true` | Promote the table to global scope when it is created, so other sessions can read it |
| `CAS_PERSIST_ON_SHUTDOWN` | `false` | On graceful shutdown, save the table to the caslib's data source |
| `CAS_PERSIST_FORMAT` | `parquet` | File extension and format used when persisting, for example `parquet` or `sashdat` |

## Stream processing

| Variable | Default | Description |
|---|---|---|
| `BATCH_SIZE` | `500` | Maximum messages per poll, and therefore per CAS write |
| `BATCH_TIMEOUT_SECONDS` | `5` | Maximum time to wait while filling a batch |
| `MESSAGE_FORMAT` | `json` | `json` (each payload must be a JSON object) or `raw` (payload stored as text in a `value` column) |
| `LOG_LEVEL` | `INFO` | Python log level: `DEBUG`, `INFO`, `WARNING`, `ERROR` |

## Synthetic producer

The producer (`python -m producer.main`) reads all the `KAFKA_*` variables above for connectivity, plus these:

| Variable | Default | Description |
|---|---|---|
| `PRODUCER_RATE_PER_SECOND` | `50` | Target send rate (best effort, evenly paced) |
| `PRODUCER_DURATION_SECONDS` | unset | Stop after N seconds |
| `PRODUCER_TOTAL_MESSAGES` | unset | Stop after N messages |
| `PRODUCER_NUM_ACCOUNTS` | `5000` | Size of the card/account pool |
| `PRODUCER_NUM_MERCHANTS` | `800` | Size of the merchant pool |
| `PRODUCER_FRAUD_RATE` | `0.01` | Probability that a given emission starts a fraud burst |
| `PRODUCER_SEED` | unset | Seed for the random generator, for reproducible runs |
| `PRODUCER_FLUSH_EVERY` | `500` | Log a progress line every N messages |
| `KAFKA_DEBUG` | unset | librdkafka debug contexts, for example `broker,security`, for troubleshooting connections |
| `LOG_LEVEL` | `INFO` | Python log level |

If neither `PRODUCER_DURATION_SECONDS` nor `PRODUCER_TOTAL_MESSAGES` is set, the producer runs until it gets `SIGTERM` / `SIGINT`.

The producer's Kafka client is configured with `acks=all`, idempotence enabled, and `zstd` compression.

## Example ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: swat-streaming-config
  namespace: swat-streaming
data:
  KAFKA_BOOTSTRAP_SERVERS: "kafka-broker-1.example.com:9093,kafka-broker-2.example.com:9093"
  KAFKA_TOPIC: "viya.events"
  KAFKA_GROUP_ID: "swat-streaming-viya-events"
  KAFKA_SECURITY_PROTOCOL: "SASL_SSL"
  KAFKA_SASL_MECHANISM: "SCRAM-SHA-512"
  KAFKA_SSL_CA_LOCATION: "/etc/kafka/certs/ca.crt"
  KAFKA_AUTO_OFFSET_RESET: "latest"

  CAS_HOST: "https://viya.example.com/cas-shared-default-http"
  CAS_PROTOCOL: "https"
  CAS_CASLIB: "Public"
  CAS_TABLE_NAME: "kafka_stream"
  CAS_TABLE_PROMOTE: "true"
  CAS_SSL_CA_LIST: "/etc/cas/certs/ca-bundle.crt"
  CAS_PERSIST_ON_SHUTDOWN: "false"

  BATCH_SIZE: "500"
  BATCH_TIMEOUT_SECONDS: "5"
  MESSAGE_FORMAT: "json"
  LOG_LEVEL: "INFO"
```
