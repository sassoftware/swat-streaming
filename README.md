<!--
Use this template to structure your project's README.md file.
Not all sections are required, though we recommend including as many as possible.

* Project name (required)
* Overview (required)
  * What's new (optional)
  * Prerequisites (optional)
* Installation (required)
  * Getting Started (optional)
  * Running (optional)
  * Examples (optional)
  * Troubleshooting (optional)
* Contributing (required)
* License (required)
* Third-Party Dependencies (required)
* Additional Resources (required)

Consider using a table of contents to make lengthy and complex README files easier to navigate.
-->

# SWAT Streaming Container
<!--
Replace `PROJECT_NAME` with your project's name.
Your project's name is the first thing people will see when browsing your project.
-->

## Overview
<!--
Include a brief project description, written from the perspective of the value your project provides users.
Be sure to define terms; links to resources where necessary and appropriate.
A good overview is clear, short, and to the point.
-->

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
<!--
If applicable to your project, list new features you want users to be aware of.
This section might supplement the Changelog file from the repository and only highlight important changes.
-->

This is a new project and the first release. 

### Prerequisites
<!--
Provide guidelines on any prerequisites that may be useful in configuring the user's environment.
Prerequisites typically take the form of a list of required software that must be available.
Each piece of software might require its own setup steps.
Use lists and subtopics as appropriate.
-->

The swat-streaming container needs to be able to reach the SAS Viya CAS server via REST endpoint.

## Installation
<!--
Provide step-by-step instructions for installing your software project.
Use subtopics and screenshots as appropriate.
-->
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

### Getting Started
<!--
Provide users with initial steps for getting started using your project after they have installed it.
This is a good place to include screenshots, animated GIFs, or short example videos.
-->

### Running
<!--
Provide users with steps for running your project after they have installed it.
This is a good place to include screenshots, recordings, or short usage videos.
-->

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

<!--
### Examples
Provide additional examples of using the software, or point to further documentation. 
Make learning and using your project as easy as possible!
-->

<!--
### Troubleshooting
Provide workarounds and solutions to known problems.
Organize troubleshooting information using subtopics, as appropriate.
-->

## Contributing
<!--
Specify whether your project accepts contributions.
Language you use in this section should mirror the language in your CONTRIBUTING.md file.
Use the default text below if you accept contributions.
If you do not accept contributions, note that here.
-->

<!-- Use this text if your project is not accepting contributions.
Maintainers are not currently accepting patches and contributions to this project.-->

<!--Use this text if your project is accepting contributions.-->
Maintainers are accepting patches and contributions to this project.
Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details about submitting contributions to this project.

## License
<!--
Choose from the default text already in place below.
Do not alter the text without prior approval from SAS Legal and the Open Source Program Office.
-->

This project is licensed under the [Apache 2.0 License](LICENSE).

<!--
Substitute the text below if your project contains trademarked elements not intended for license under the open source license.
In this case, add all trademarked images to the same folder and replace `XXX` in the statement with the name of that folder.

Except for the the contents of the `XXX` folder, this project is licensed under the [Apache 2.0 License](LICENSE). Elements in the `XXX` folder are owned by SAS and are not released under an open source license. SAS and all other SAS Institute Inc. product or service names are registered trademarks or trademarks of SAS Institute Inc. in the USA and other countries. ® indicates USA registration.
-->
<!--
If your projects ships with/as a container image for a package registry, keep the following statement in place.
Otherwise, remove it.
-->
As with any container image, direct and indirect dependencies are governed by their own licenses.
Users of the published container image are responsible for ensuring that their use complies with all applicable licenses.

## Third-Party Dependencies
<!--
List all third-party dependencies required for using your project, then list and link to each dependency's license.
Use the table to organize your dependency list.
Do not include version numbers.
Consult (and then remove) the provided example.
-->
This project requires the following dependencies.

| Dependency | License |
| ---------- | ------- |
| `github.com/sassoftware/dpmm` | [Apache 2.0](https://github.com/sassoftware/dpmm/blob/main/LICENSE) |

## Additional Resources
<!--
Include any additional materials users may need or find useful when using your software. Additional resources might include:

* Documentation links
* Research papers
* Blog posts
* Articles from SAS Communities
* Other relevant documentation (i.e., from Tech Support, Education, etc.)
-->

* [`SAS Scripting Wrapper for Analytics Transfer (SWAT)`](https://sassoftware.github.io/python-swat/)
* [`Confluent's Python Client for Apache Kafka®`](https://pypi.org/project/confluent-kafka/)
* [`CAS REST`](https://developer.sas.com/rest-apis/cas)
