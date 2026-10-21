---
sidebar_position: 3
title: Deployment
description: Build the container image and deploy swat-streaming to Kubernetes.
---

# Deployment

This guide walks through building the container image and deploying the `swat-streaming` workload into a Kubernetes cluster that can reach both your Kafka brokers and the SAS Viya CAS endpoint.

## Prerequisites

| Requirement | Notes |
|---|---|
| Kafka cluster | Reachable from the cluster; TLS strongly recommended |
| Kafka topic | Pre-created; the partition count sets the maximum parallelism |
| Kafka service account (consumer) | Read access to the topic, member of the consumer group |
| Kafka service account (producer, optional) | Write + Describe ACL on the topic. Only needed for the [synthetic producer](testing.md) |
| SAS Viya | Reachable HTTPS endpoint of the CAS server, for example `https://viya.example.com/cas-shared-default-http` |
| Viya service user | SAS user with permission on the target caslib (see [CAS authorization](authentication.md#cas-authorization)) |
| OAuth client (optional, recommended) | Registered in SAS Logon Manager to issue access tokens |
| Kubernetes 1.24+ | With a working `kubectl` / `kustomize` workflow |
| Container registry | To host the built image |

## 1. Build the container image

From the repository root:

```bash
docker build -t ghcr.io/your-org/swat-streaming:0.1.0 .
docker push ghcr.io/your-org/swat-streaming:0.1.0
```

What the image contains:

- Python 3.11 slim base, running as UID/GID `10001:10001`
- `tini` as PID 1, so signals are forwarded cleanly
- `librdkafka1`, used by `confluent-kafka`
- `libnuma1` and `libgomp1`. The SWAT wheel bundles SAS TK shared libraries and loads them **at import time**, even when only the REST protocol is used. Without `libnuma1`, `import swat` fails with `Cannot dlopen [tkmk.so] : [libnuma.so.1: cannot open shared object file]`.
- Both entrypoints: `python -m app.main` (the default, the consumer) and `python -m producer.main` (the test producer)

:::tip REST vs. binary protocol
By default the container talks to CAS over HTTPS with SWAT's REST protocol. This is the recommended setup for streaming because it passes through ingress controllers and firewalls cleanly and authenticates with OAuth. To use the CAS binary protocol (port 5570) instead, set `CAS_PROTOCOL=cas` and point `CAS_HOST` / `CAS_PORT` at the CAS controller. No image changes are needed.
:::

Update `image:` in `k8s/deployment.yaml` (and `k8s/producer-job.yaml`) to match the tag you pushed. For production, pin the image by digest.

## 2. Configure the workload

Non-sensitive settings live in the `swat-streaming-config` ConfigMap (`k8s/configmap.yaml`). Credentials and TLS material live in Secrets. At minimum, edit:

| Variable | Purpose |
|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | Comma-separated `host:port` list |
| `KAFKA_TOPIC` | Topic to subscribe to |
| `KAFKA_GROUP_ID` | Consumer group; controls offset tracking |
| `CAS_HOST` | Full URL of the CAS HTTP endpoint |
| `CAS_CASLIB` | Target caslib (must already exist, for example `Public`) |
| `CAS_TABLE_NAME` | In-memory CAS table that receives the stream |

The [configuration reference](configuration.md) lists every supported variable.

:::info Tuning batches
CAS appends have noticeable per-call overhead, so prefer larger batches (500–5000 rows) over small ones. Combine `BATCH_SIZE` with `BATCH_TIMEOUT_SECONDS` so that batches still flush regularly when traffic is low.
:::

Also adjust `k8s/networkpolicy.yaml`. The shipped policy allows egress to `10.0.0.0/8` on ports 9093 (Kafka), 443 and 8777 (CAS REST), plus DNS. Narrow the CIDRs to your actual broker and Viya networks, and change the ports if yours differ.

```bash
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/configmap.yaml
```

## 3. Provision secrets

The Deployment expects two Secrets in the `swat-streaming` namespace:

| Secret | Keys | Mounted at |
|---|---|---|
| `kafka-credentials` | `username`, `password`, `ca.crt` | `/etc/kafka/credentials/`, `/etc/kafka/certs/` |
| `cas-credentials` | `token` (or `username` + `password`), `ca-bundle.crt` | `/etc/cas/credentials/`, `/etc/cas/certs/` |

For a controlled lab environment:

```bash
kubectl -n swat-streaming create secret generic kafka-credentials \
  --from-literal=username='svc-viya-consumer' \
  --from-literal=password='REPLACE_ME' \
  --from-file=ca.crt=./kafka-ca.crt

kubectl -n swat-streaming create secret generic cas-credentials \
  --from-literal=token='REPLACE_ME_OAUTH_TOKEN' \
  --from-file=ca-bundle.crt=./viya-ca-bundle.crt
```

:::warning
`k8s/secret.example.yaml` is intentionally **not** part of the Kustomization. Never commit real Secret manifests. For production, use External Secrets Operator, Sealed Secrets, or Vault Agent Injector. See [Authentication](authentication.md#secret-delivery-into-the-pod).
:::

If you authenticate to CAS with username/password instead of a token, edit `k8s/deployment.yaml`: remove `CAS_TOKEN_FILE`, add `CAS_USERNAME_FILE` and `CAS_PASSWORD_FILE`, and adjust the `cas-creds` volume items to match.

## 4. Deploy

```bash
kubectl apply -k k8s/
kubectl -n swat-streaming rollout status deploy/swat-streaming
kubectl -n swat-streaming logs -f deploy/swat-streaming
```

A healthy consumer logs a startup line, then one line per batch:

```json
{"ts":"...","level":"INFO","logger":"app.main","message":"swat-streaming starting","topic":"viya.events","table":"Public.kafka_stream","batch_size":500}
{"ts":"...","level":"INFO","logger":"app.cas_writer","message":"CAS session established","existing_target_table":false}
{"ts":"...","level":"INFO","logger":"app.main","message":"Batch processed","received":500,"written":500,"parse_failures":0}
```

The Deployment uses the `Recreate` strategy, a non-root security context with a read-only root filesystem, all capabilities dropped, and `automountServiceAccountToken: false`.

## 5. Verify the CAS table

If there is no live data on the topic yet, run the [synthetic producer](testing.md) first.

From SAS Studio, or any Python environment with SWAT:

```python
import swat

conn = swat.CAS("https://viya.example.com/cas-shared-default-http",
                username="...", password="...")
conn.table.tableInfo(caslib="Public")
conn.CASTable("kafka_stream", caslib="Public").head(20)
```

With `CAS_TABLE_PROMOTE=true`, the table appears under the configured caslib for every Viya user with read permission on it.

## Next steps

- Scale out, persist the table, and handle schema changes: [Operations](operations.md)
- Harden credentials and plan rotation: [Authentication](authentication.md)
