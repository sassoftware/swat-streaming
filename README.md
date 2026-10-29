# SWAT Streaming Container

## Overview

A containerized bridge that consumes a Kafka topic and writes the messages
into a SAS Viya CAS table using the SWAT Python client. Built for
deployments where SAS Viya cannot subscribe to Kafka natively and you want
streaming data available in-memory in CAS for downstream analytics.

```
 ┌────────┐    ┌─────────────────────────────┐    ┌──────────────────┐
 │ Kafka  │───▶│  swat-streaming container   │───▶│  SAS Viya / CAS  │
 │ topic  │    │  (confluent-kafka + SWAT)   │    │  in-memory table │
 └────────┘    └─────────────────────────────┘    └──────────────────┘
```

The container will write permanently data to the CAS table. Consider to offload 
and aggregate the data in the table in regular time frames, as memory is limited at some stage. 
If you plan to ingest massive amounts of data, please reach out to SAS if other 
solutions might fit better. 

### What's in this repo

| Path | Purpose |
|---|---|
| `app/` | Python application (Kafka consumer + SWAT writer + batching loop) |
| `producer/` | Synthetic Kafka producer for end-to-end testing (card-fraud transactions) |
| `Dockerfile` | Slim, non-root container image — ships both consumer and producer |
| `requirements.txt` | Pinned Python dependencies |
| `k8s/` | Namespace, ConfigMap, Deployment, NetworkPolicy, producer Job, example Secret, Kustomization |
| `docs/SETUP.md` | Step-by-step build + deploy guide |
| `docs/AUTHENTICATION.md` | Kafka SASL/SSL + CAS OAuth + Kubernetes secret strategies |
| `docs/TESTING.md` | Running the synthetic producer + verifying data in CAS |

### What's New

This is a new project and the first release. 

### Prerequisites

The swat-streaming container needs to be able to reach the SAS Viya CAS server via REST endpoint.

## Installation

```
1. Build and push the image
docker build -t ghcr.io/your-org/swat-streaming:0.1.0 .
docker push ghcr.io/your-org/swat-streaming:0.1.0

2. Edit k8s/configmap.yaml with your Kafka brokers, topic, and CAS host

### 3. Create the namespace and secrets (see docs/AUTHENTICATION.md)
kubectl apply -f k8s/namespace.yaml
kubectl -n swat-streaming create secret generic kafka-credentials \
  --from-literal=username='svc-viya-consumer' \
  --from-literal=password='REPLACE_ME' \
  --from-file=ca.crt=./kafka-ca.crt
kubectl -n swat-streaming create secret generic cas-credentials \
  --from-literal=token='REPLACE_ME_OAUTH_TOKEN' \
  --from-file=ca-bundle.crt=./viya-ca-bundle.crt

### 4. Deploy
kubectl apply -k k8s/
kubectl -n swat-streaming logs -f deploy/swat-streaming

### 5. (Optional) Fire synthetic transactions at the topic for testing
kubectl apply -f k8s/producer-job.yaml
kubectl -n swat-streaming logs -f job/swat-streaming-producer
```

See [`docs/TESTING.md`](docs/TESTING.md) for the producer's message schema
and injectable fraud patterns.

### Running

All configuration is via environment variables. The full list is in
`k8s/configmap.yaml`; the key knobs:

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | *(required)* | Comma-separated `host:port` list |
| `KAFKA_TOPIC` | *(required)* | Topic to consume |
| `KAFKA_GROUP_ID` | *(required)* | Consumer group ID |
| `KAFKA_SECURITY_PROTOCOL` | `SASL_SSL` | `SASL_SSL`, `SSL`, `SASL_PLAINTEXT`, `PLAINTEXT` |
| `KAFKA_SASL_MECHANISM` | `SCRAM-SHA-512` | `SCRAM-SHA-512`, `SCRAM-SHA-256`, `PLAIN` |
| `CAS_HOST` | *(required)* | HTTPS URL of the CAS REST endpoint |
| `CAS_CASLIB` | `CASUSER` | Caslib that owns the target table |
| `CAS_TABLE_NAME` | `kafka_stream` | In-memory CAS table name |
| `CAS_TABLE_PROMOTE` | `true` | Promote to global scope so other sessions can read |
| `BATCH_SIZE` | `500` | Max messages per CAS upload |
| `BATCH_TIMEOUT_SECONDS` | `5` | Flush even if batch is not full |
| `MESSAGE_FORMAT` | `json` | `json` (top-level object) or `raw` |
| `LOG_LEVEL` | `INFO` | Python log level |

The synthetic producer reuses `KAFKA_*` for connectivity and adds its own
`PRODUCER_*` tunables (rate, duration, fraud injection, seed). See the
**Tunables** table in [`docs/TESTING.md`](docs/TESTING.md#33-tunables).


| Hop | Recommended | Configured via |
|---|---|---|
| Kafka | SASL/SCRAM-SHA-512 over TLS | `kafka-credentials` Secret (username, password, CA) |
| CAS  | OAuth bearer token from SAS Logon | `cas-credentials` Secret (token, CA bundle) |

mTLS for Kafka and username/password for CAS are also supported. See
[`docs/AUTHENTICATION.md`](docs/AUTHENTICATION.md) for the full discussion
including secret-rotation patterns and which Kubernetes secret-management
tool to pick (External Secrets Operator, Sealed Secrets, Vault Agent, CSI
Secrets Store).

## Contributing

Maintainers are accepting patches and contributions to this project.
Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details about submitting contributions to this project.

## License

This project is licensed under the [Apache 2.0 License](LICENSE).

As with any container image, direct and indirect dependencies are governed by their own licenses.
Users of the published container image are responsible for ensuring that their use complies with all applicable licenses.

## Third-Party Dependencies

This project requires the following dependencies.

| Dependency | License |
| ---------- | ------- |
| `github.com/sassoftware/dpmm` | [Apache 2.0](https://github.com/sassoftware/dpmm/blob/main/LICENSE) |

## Additional Resources

* [`SAS Scripting Wrapper for Analytics Transfer (SWAT)`](https://sassoftware.github.io/python-swat/)
* [`Confluent's Python Client for Apache Kafka®`](https://pypi.org/project/confluent-kafka/)
* [`CAS REST`](https://developer.sas.com/rest-apis/cas)
