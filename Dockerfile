FROM python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu:/lib/x86_64-linux-gnu

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        ca-certificates \
        librdkafka1 \
        libssl3 \
        libnuma1 \
        libgomp1 \
        tini \
 && rm -rf /var/lib/apt/lists/*

RUN groupadd --system --gid 10001 streamer \
 && useradd  --system --uid 10001 --gid streamer --home-dir /app --shell /sbin/nologin streamer

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY app /app/app
COPY producer /app/producer

USER 10001:10001

# Default entrypoint runs the consumer. Override `command` in the pod spec to
# run the test producer instead, e.g.:
#   command: ["python", "-m", "producer.main"]
ENTRYPOINT ["/usr/bin/tini", "--", "python", "-m", "app.main"]
