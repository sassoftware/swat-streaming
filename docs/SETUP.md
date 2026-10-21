# Setup Guide

This document walks through building the container image and deploying the
`swat-streaming` workload into a Kubernetes cluster that can reach both your
Kafka brokers and the SAS Viya CAS HTTP endpoint.

---

## 1. Prerequisites

| Requirement | Notes |
|---|---|
| Kafka cluster | Reachable from the cluster; TLS strongly recommended |
| Kafka topic | Pre-created; partition count drives max parallelism |
| Kafka service account (consumer) | Read access to the topic, member of the consumer group |
| Kafka service account (producer, optional) | Write+Describe ACL on the topic — only if you plan to run the synthetic producer for testing (see `docs/TESTING.md`) |
| SAS Viya | Reachable HTTPS endpoint of the CAS server (e.g. `https://viya.example.com/cas-shared-default-http`) |
| Viya service user | SAS user with permission on the target CASLIB; ideally restricted to `LIMITED PROMOTE` / `WRITE` on that scope |
| OAuth client (optional, recommended) | Registered in SAS Logon Manager to issue access tokens |
| Kubernetes 1.24+ | With a working `kubectl`/`kustomize` workflow |
| Container registry | To host the built image |

---

## 2. Build the container image

```bash
# from the repo root
docker build -t ghcr.io/your-org/swat-streaming:0.1.0 .

# push to your registry
docker push ghcr.io/your-org/swat-streaming:0.1.0
```

The image:
- runs as UID/GID `10001:10001`
- uses `tini` as PID 1 for clean signal forwarding
- ships with `librdkafka1` (used by `confluent-kafka`)
- ships with `libnuma1` and `libgomp1` — the SWAT pip wheel bundles SAS's
  TK shared libraries and initializes them at *import time* even when only
  the REST protocol is used. Without `libnuma1` SWAT fails to import with
  `Cannot dlopen [tkmk.so] : [libnuma.so.1: cannot open shared object file]`
- talks to CAS over HTTPS via SWAT's REST protocol — recommended for
  streaming because it traverses ingress/firewalls cleanly and authenticates
  with OAuth

If you want to use the CAS binary protocol (port 5570) instead, set
`CAS_PROTOCOL=cas` and point `CAS_HOST`/`CAS_PORT` at the CAS controller's
binary port. The same TK libraries are used; no additional image changes
are required.

---

## 3. Configure the workload

Configuration is split between a **ConfigMap** (non-sensitive, declarative)
and **Secrets** (credentials + TLS material). See `k8s/configmap.yaml` for the
full list of supported environment variables; the most important ones:

| Variable | Purpose |
|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | Comma-separated `host:port` list |
| `KAFKA_TOPIC` | Topic to subscribe to |
| `KAFKA_GROUP_ID` | Consumer group — controls offset tracking |
| `KAFKA_SECURITY_PROTOCOL` | `SASL_SSL` (recommended), `SSL`, `SASL_PLAINTEXT`, `PLAINTEXT` |
| `KAFKA_SASL_MECHANISM` | `SCRAM-SHA-512`, `SCRAM-SHA-256`, or `PLAIN` |
| `CAS_HOST` | Full URL of the CAS HTTP endpoint |
| `CAS_CASLIB` | Target caslib (must already exist, e.g. `Public`) |
| `CAS_TABLE_NAME` | In-memory CAS table to receive the stream |
| `CAS_TABLE_PROMOTE` | `true` → table is global-scope and visible to other sessions |
| `BATCH_SIZE` | Max messages per CAS write |
| `BATCH_TIMEOUT_SECONDS` | Flush even if `BATCH_SIZE` not reached |
| `MESSAGE_FORMAT` | `json` (object payloads) or `raw` |

**Tuning batches.** CAS appends have non-trivial per-call overhead, so prefer
larger batches (500–5000 rows) over per-message writes. Combine `BATCH_SIZE`
and `BATCH_TIMEOUT_SECONDS` so low-traffic periods still flush regularly.

Apply non-sensitive configuration:

```bash
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/configmap.yaml
```

---

## 4. Provision secrets

See `docs/AUTHENTICATION.md` for the full discussion. Quick path for a
controlled lab:

```bash
kubectl -n swat-streaming create secret generic kafka-credentials \
  --from-literal=username='svc-viya-consumer' \
  --from-literal=password='REPLACE_ME' \
  --from-file=ca.crt=./kafka-ca.crt

kubectl -n swat-streaming create secret generic cas-credentials \
  --from-literal=token='REPLACE_ME_OAUTH_TOKEN' \
  --from-file=ca-bundle.crt=./viya-ca-bundle.crt
```

For real environments use External Secrets Operator, Sealed Secrets, or Vault
Agent Injector instead of `kubectl create secret`.

---

## 5. Deploy

```bash
kubectl apply -k k8s/
kubectl -n swat-streaming rollout status deploy/swat-streaming
kubectl -n swat-streaming logs -f deploy/swat-streaming
```

You should see JSON log lines for each batch:

```json
{"ts":"...","level":"INFO","logger":"app.main","message":"Batch processed","received":500,"written":500,"parse_failures":0}
```

---

## 6. Verify the CAS table

If you do not yet have live data on the topic, run the synthetic producer
first to seed it with card-transaction messages (including labeled fraud
bursts). See [`docs/TESTING.md`](TESTING.md) for the Job manifest and the
message schema.

From SAS Studio, SAS Enterprise Guide, or a notebook with SWAT:

```python
import swat
conn = swat.CAS("https://viya.example.com/cas-shared-default-http",
                username="...", password="...")
conn.table.tableInfo(caslib="Public")
conn.CASTable("kafka_stream", caslib="Public").head(20)
```

If `CAS_TABLE_PROMOTE=true`, the table appears under the configured caslib for
all Viya users with read permission on it.

---

## 7. Scaling

- **Vertical**: increase `BATCH_SIZE` and the memory limit. CAS is the
  bottleneck before Kafka.
- **Horizontal**: increase `replicas` up to the partition count of the topic.
  Multiple pods join the same `KAFKA_GROUP_ID`, so Kafka rebalances partitions
  across them automatically. Do not exceed the partition count — extra pods
  sit idle.
- **Multiple topics → multiple CAS tables**: deploy multiple instances of
  this Deployment, each with its own ConfigMap.

---

## 8. Test the pipeline end-to-end

A synthetic Kafka producer is bundled in the same image. It generates a
realistic stream of card-payment transactions with injectable fraud
patterns, suitable for proving the consumer → CAS path end-to-end before
you point it at real traffic.

```bash
kubectl apply -f k8s/producer-job.yaml
kubectl -n swat-streaming logs -f job/swat-streaming-producer
```

The producer requires a Kafka principal with Write+Describe ACL on the
topic — see [`docs/TESTING.md`](TESTING.md) for tunables, message schema,
and the recommended *separate* `kafka-producer-credentials` Secret pattern.

---

## 9. Operational notes

- **At-least-once delivery.** The app commits Kafka offsets only after a
  successful CAS write. A pod crash mid-batch will replay the batch on
  restart; downstream consumers of the CAS table should tolerate duplicates
  (or you can `deduplicate` in Viya using the Kafka offset columns the app
  attaches to every row).
- **Schema drift.** `upload_frame` infers types from the first batch.
  Changing the message schema mid-stream can cause upload errors. Either pin
  a schema, restart the pod after a schema change, or set
  `CAS_TABLE_PROMOTE=false` and pre-create the table with explicit types.
- **Persistence.** A promoted CAS table lives in CAS memory until the CAS
  server restarts. Set `CAS_PERSIST_ON_SHUTDOWN=true` to save the table to
  the caslib on graceful shutdown, or schedule a separate Viya job that
  periodically calls `table.save`.
