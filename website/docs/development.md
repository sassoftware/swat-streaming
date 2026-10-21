---
sidebar_position: 8
title: Development
description: Run swat-streaming locally, understand the code layout, and contribute.
---

# Development

## Code layout

```
app/
  main.py            # StreamRunner: batch loop, signal handling, entrypoint
  config.py          # Env-var configuration dataclasses (Kafka, CAS, stream)
  kafka_consumer.py  # KafkaSource: poll batches, commit offsets
  message_parser.py  # parse_message (json/raw) and enrich (Kafka metadata)
  cas_writer.py      # CASWriter: connect, initial load, staged append, save
  logging_setup.py   # JSON logging to stdout
producer/
  main.py            # Producer loop: rate limiting, stop conditions, delivery callbacks
  config.py          # PRODUCER_* settings and the Kafka producer configuration
  generator.py       # TransactionGenerator: accounts, merchants, fraud bursts
```

The producer reuses `app.config.KafkaConfig` and `app.logging_setup`, so changes there affect both entrypoints.

## Running locally

Requirements: Python 3.11, plus a Kafka broker and a CAS server you can reach. The container image is the reference environment. On Linux, the SWAT wheel also needs `libnuma1` and `libgomp1` (see [Deployment](deployment.md#1-build-the-container-image)).

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

For a throwaway single-node Kafka broker on `localhost:9092`, you can use the official Apache Kafka image:

```bash
docker run -d --name kafka -p 9092:9092 apache/kafka:3.8.0
```

Start the consumer against it:

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_TOPIC=viya.events
export KAFKA_GROUP_ID=swat-streaming-dev
export KAFKA_SECURITY_PROTOCOL=PLAINTEXT
export KAFKA_AUTO_OFFSET_RESET=earliest

export CAS_HOST=https://viya.example.com/cas-shared-default-http
export CAS_TOKEN_FILE=$HOME/.viya/token   # or CAS_USERNAME / CAS_PASSWORD
export CAS_CASLIB=CASUSER
export CAS_TABLE_NAME=kafka_stream_dev
export LOG_LEVEL=DEBUG

python -m app.main
```

In a second terminal, with the same `KAFKA_*` variables, generate traffic:

```bash
PRODUCER_RATE_PER_SECOND=20 PRODUCER_TOTAL_MESSAGES=1000 PRODUCER_SEED=1 \
  python -m producer.main
```

Or run both from the image you built:

```bash
docker build -t swat-streaming:dev .
docker run --rm --network host --env-file dev.env swat-streaming:dev                          # consumer
docker run --rm --network host --env-file dev.env swat-streaming:dev python -m producer.main  # producer
```

:::tip
Leave `dev.env` and any token files out of version control. The repository's `.dockerignore` already excludes `.env` and `.env.*`.
:::

## Coding conventions

- **Configuration** goes through the dataclasses in `app/config.py`. Add new settings as fields with a `from_env()` mapping, use `_file_or_env` for anything secret, and document new variables in the [configuration reference](configuration.md) and `k8s/configmap.yaml`.
- **Logging** uses structured context: `log.info("Message", extra={"key": value})`, not string formatting. Never log credentials.
- **Offset commits** happen only after a successful CAS write. Keep that ordering whenever you change the loop in `app/main.py`.
- **Dependencies** are pinned exactly in `requirements.txt`.
- **Source file headers.** Every source file must start with the SAS copyright header:

  ```
  Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
  SPDX-License-Identifier: Apache-2.0
  ```

## Working on this documentation

The documentation site is built with [Docusaurus](https://docusaurus.io/) from the `website/` directory. The pages are Markdown (MDX) files in `website/docs/`, and the sidebar order comes from each page's `sidebar_position` front matter.

```bash
cd website
npm ci
npm start        # live-reloading dev server at http://localhost:3000/swat-streaming/
npm run build    # production build into website/build (fails on broken links)
```

On every push to `main`, the `Deploy Doc Website to GitHub Pages` workflow builds the site and publishes it to GitHub Pages.

Diagrams are written in [Mermaid](https://mermaid.js.org/) inside a fenced `mermaid` code block.

## Contributing

Maintainers accept patches and contributions to this project. Please read the [contribution guidelines](https://github.com/sassoftware/swat-streaming/blob/main/CONTRIBUTING.md) and the [SAS Open Source Contributor Handbook](https://sassoftware.github.io/contributor-handbook.html).

- Contributions must follow the [Contributor Agreement](https://github.com/sassoftware/swat-streaming/blob/main/ContributorAgreement.txt).
- External contributors must sign off their commits (`git commit -s`). The DCO check enforces this on pull requests.
- All submissions, including those from project members, are reviewed through GitHub pull requests.
