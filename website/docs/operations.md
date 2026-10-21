---
sidebar_position: 7
title: Operations
description: Scaling, persistence, schema changes, monitoring, and troubleshooting for swat-streaming.
---

# Operations

## Scaling

- **Vertical.** Increase `BATCH_SIZE` and the pod's memory limit. CAS usually becomes the bottleneck before Kafka does, and larger batches spread the per-call overhead over more rows. The default resources are 200m CPU / 512Mi requested and 1 CPU / 1Gi limit.
- **Horizontal.** Increase `replicas` in `k8s/deployment.yaml`, up to the topic's partition count. All pods join the same `KAFKA_GROUP_ID`, and Kafka spreads partitions across them. Pods beyond the partition count sit idle.
- **Multiple topics.** Deploy one instance per topic, each with its own ConfigMap, consumer group, and target table.

:::caution Creating the table with several replicas
A replica that doesn't find the target table at startup creates it with `replace=True` on its first batch. If several replicas start before the table exists, they can each create the table and overwrite each other's first batch. Start with a single replica until the table exists, then scale up.
:::

## Persistence

A promoted CAS table lives **in memory** until it is dropped or the CAS server restarts. To keep the data:

- Set `CAS_PERSIST_ON_SHUTDOWN=true` to save the table to the caslib's data source as `<table>.<CAS_PERSIST_FORMAT>` (Parquet by default) when the pod shuts down gracefully. This doesn't run if the pod crashes or is killed with `SIGKILL`.
- Or schedule a separate Viya job that calls `table.save` regularly. This is the more robust option for production.

When the consumer restarts and finds the table already in CAS, it appends to it. It doesn't reload the persisted file. If CAS has restarted in the meantime, load the saved file back into memory (and promote it) before you restart the consumer. Otherwise the consumer creates a new table from the next batch, and with `CAS_PERSIST_ON_SHUTDOWN=true` the next shutdown overwrites the saved file with that new, smaller table.

:::note Session-scoped tables
With `CAS_TABLE_PROMOTE=false`, the table belongs to the consumer's CAS session. Other users can't see it, and it disappears when the pod restarts. Use this only for testing.
:::

## Schema changes

`upload_frame` infers CAS column types from the **first** batch, and every later batch is appended to that schema. This means:

- A **new field** in later messages, or a **different type** for an existing field (for example, a number that becomes a string), can make the append fail. The batch is retried and then the pod exits with code 1. Because the offset isn't committed, the pod crash-loops on the same batch.
- Fields that are missing from some messages become missing values. Fields that are missing from *every* message in the first batch don't exist in the table at all.

Ways to handle schema changes:

- Keep message schemas stable, or version them onto separate topics and tables.
- After an intentional schema change, drop (or rename) the target table and restart the consumer so it creates a new one.
- Pre-create the target table in CAS with explicit column types. The consumer appends to an existing table instead of creating one.

## Duplicates

Delivery is at-least-once (see [Delivery guarantees](architecture.md#delivery-guarantees)). Rows can be duplicated when a pod stops between a CAS write and the offset commit, or when a retried write partly succeeded. Deduplicate on `_kafka_topic`, `_kafka_partition`, `_kafka_offset`, for example with a periodic `deduplication.deduplicate` action or in downstream data preparation.

## Monitoring

Every log line is JSON on stdout, so you can ship the logs to any log aggregator and alert on them. Useful signals:

| Log message | Level | What it means |
|---|---|---|
| `Batch processed` | INFO | One line per batch, with `received`, `written` and `parse_failures`. Use it to track throughput and parse-error rate |
| `Batch appended to CAS` / `Initial CAS table created` | INFO | Per-batch CAS write details (`rows`, `table`, `caslib`) |
| `Skipping unparseable message` | WARNING | A message wasn't valid for `MESSAGE_FORMAT`. Includes `topic`, `partition`, `offset` and the error |
| `Fatal error in stream runner` | ERROR | The process is about to exit with code 1. Includes the stack trace |

You can track consumer lag with your usual Kafka tooling, for example `kafka-consumer-groups.sh --describe --group <KAFKA_GROUP_ID>`.

:::note Liveness probe
The liveness probe in `k8s/deployment.yaml` only checks that the Python interpreter can start inside the container. It doesn't detect a stalled consumer. Fatal errors make the process exit, which Kubernetes does notice, but you should still alert on consumer lag or on `Batch processed` lines stopping.
:::

## Troubleshooting

| Symptom | Likely cause and fix |
|---|---|
| Pod exits right away with `Configuration error: Required environment variable X is not set` (exit code 2) | A required variable is missing from the ConfigMap. Required: `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, `KAFKA_GROUP_ID`, `CAS_HOST` |
| `Cannot dlopen [tkmk.so] : [libnuma.so.1 ...]` on startup | The image is missing `libnuma1`. Build from the provided `Dockerfile`, or install `libnuma1` and `libgomp1` in your own base image |
| Pod fails to start with a `FileNotFoundError` for `/etc/.../token` or similar | A `*_FILE` variable points at a file that isn't mounted. Check that the Secret exists and has the expected keys |
| Kafka authentication or TLS errors | Check the SASL mechanism, the credentials, and that `KAFKA_SSL_CA_LOCATION` contains the CA that signed the broker certificates. Check the NetworkPolicy ports (9093 by default) |
| CAS connection fails with a TLS error | `CAS_SSL_CA_LIST` must point to a bundle that contains the Viya CA |
| CAS connection fails with 401 / authentication errors | The OAuth token has probably expired. Refresh the Secret and restart the Deployment (see [Rotation](authentication.md#rotation)) |
| `CAS append failed` and the pod crash-loops | The message schema no longer matches the table. See [Schema changes](#schema-changes) |
| Consumer is running, but no `Batch processed` lines appear | No new messages. With `KAFKA_AUTO_OFFSET_RESET=latest`, a **new** consumer group only sees messages produced after it joined. Use `earliest` (or a new group ID) to read existing messages |
| The table isn't visible in SAS Studio or Visual Analytics | Use `CAS_TABLE_PROMOTE=true`, and check that the viewing user has read access to the caslib |
| Consumer is evicted from the group or rebalances repeatedly | A CAS write (including retries) took longer than `KAFKA_MAX_POLL_INTERVAL_MS`. Reduce `BATCH_SIZE`, or increase the interval |
