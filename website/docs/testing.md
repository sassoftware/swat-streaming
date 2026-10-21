---
sidebar_position: 6
title: Testing with the synthetic producer
sidebar_label: Testing
description: Generate realistic card-transaction traffic with labeled fraud patterns and verify the data in CAS.
---

# Testing with the synthetic producer

The `producer/` package is a Kafka producer that generates a realistic stream of card-payment transactions for end-to-end testing. It's modeled on a **fraud-detection** use case: a fixed population of cards and accounts, mostly legitimate traffic, configurable bursts of known fraud patterns, geo-aware merchants, MCCs, and several payment channels.

The producer ships in the same container image and uses the same Kafka authentication setup as the consumer. Only the entrypoint is different: `python -m producer.main`.

## Message schema

Every record is a JSON object. Messages are keyed by `account_id`, so all transactions for one card land on the same partition and stay in order.

```json
{
  "transaction_id": "txn_8d9c7b6a5e4f3210",
  "event_time": "2026-05-27T10:42:13.812Z",
  "account_id": "acct_00004217",
  "card_id": "5499207421896013",
  "card_bin": "549920",
  "merchant_id": "mer_000241",
  "merchant_name": "REWE Supermarket",
  "merchant_mcc": "5411",
  "merchant_country": "DE",
  "merchant_city": "Berlin",
  "amount": 38.71,
  "currency": "EUR",
  "channel": "contactless",
  "device_id": null,
  "ip_address": null,
  "lat": 52.521,
  "lon": 13.404,
  "is_fraud": false,
  "fraud_pattern": null
}
```

- `card_id` passes the Luhn check but is entirely synthetic.
- `channel` is one of `chip`, `contactless`, `magstripe`, `online`, `atm`.
- `device_id` and `ip_address` are only filled in when `channel` is `online`.
- `is_fraud` and `fraud_pattern` are ground-truth labels. You can train models against them or use them to check end-to-end correctness.

Once the consumer writes these records to CAS, each row also gets the Kafka lineage columns described in [Architecture](architecture.md#the-batch-loop).

## Fraud patterns

On each emission, a fraud burst starts with probability `PRODUCER_FRAUD_RATE`. The generator picks one of five patterns and emits all of that burst's messages back to back:

| Pattern | Shape | Typical detection signal |
|---|---|---|
| `velocity` | 6–12 small online charges on one card within about 30 s | N transactions in T seconds on the same card |
| `geo_jump` | A domestic transaction, followed minutes later by one in a high-risk foreign country | Geo-impossibility check |
| `high_amount` | One charge of 25–80× the account's typical amount | Z-score on the amount distribution |
| `card_testing` | 4–9 micro-charges across different merchants | Amount + merchant entropy |
| `cnp_foreign` | A single online transaction from a high-risk country | Channel + country score |

These patterns are deliberately easy to detect. The point is to validate the **pipeline** (Kafka → CAS), not a model.

## Running the producer

### As a Kubernetes Job (recommended)

```bash
kubectl apply -f k8s/producer-job.yaml
kubectl -n swat-streaming logs -f job/swat-streaming-producer
```

By default the Job sends **50 messages/s for 10 minutes** with a **2% fraud rate** and seed `42`. To change those values, edit the `env:` block in `k8s/producer-job.yaml`. The Job uses `restartPolicy: Never` and `backoffLimit: 0`, so a configuration error fails fast instead of retrying. Finished Jobs are cleaned up after 10 minutes (`ttlSecondsAfterFinished: 600`).

:::caution Credentials
The example Job reuses the consumer's `kafka-credentials` Secret, which only works if that principal can write to the topic. In real environments, keep the consumer read-only and give the producer its own principal and Secret. See [Producer credentials](authentication.md#producer-credentials).
:::

To re-run the Job, delete it first: `kubectl -n swat-streaming delete job swat-streaming-producer`.

### Locally against a development Kafka

```bash
docker run --rm \
  -e KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
  -e KAFKA_TOPIC=viya.events \
  -e KAFKA_GROUP_ID=ignored-for-producer \
  -e KAFKA_SECURITY_PROTOCOL=PLAINTEXT \
  -e PRODUCER_RATE_PER_SECOND=200 \
  -e PRODUCER_DURATION_SECONDS=60 \
  -e PRODUCER_FRAUD_RATE=0.05 \
  -e PRODUCER_SEED=42 \
  --network host \
  ghcr.io/your-org/swat-streaming:0.1.0 \
  python -m producer.main
```

`KAFKA_GROUP_ID` is required by the shared Kafka configuration, even though the producer doesn't use it.

All producer settings are listed in the [configuration reference](configuration.md#synthetic-producer).

### Reading the producer logs

The producer logs a progress line every `PRODUCER_FLUSH_EVERY` messages and a summary at the end:

```json
{"level":"INFO","message":"Producer progress","sent":5000,"fraud_sent":412,"elapsed_s":100.1,"actual_rate":49.9}
{"level":"INFO","message":"Producer finished","sent":30000,"fraud_sent":2468,"elapsed_s":600.4}
```

`fraud_sent` counts individual fraudulent messages, not bursts. Because most patterns emit several messages, `fraud_sent / sent` is noticeably higher than `PRODUCER_FRAUD_RATE`.

If messages aren't arriving, look for `Kafka client error` lines (authentication failures, unknown topic, TLS problems) and `Delivery failed` lines. Setting `KAFKA_DEBUG=broker,security` turns on librdkafka's detailed connection logs.

## Verifying the result in CAS

After the producer finishes, the consumer should have written rows to `<CAS_CASLIB>.<CAS_TABLE_NAME>`. Some quick checks from a SWAT session:

```python
import swat

conn = swat.CAS("https://viya.example.com/cas-shared-default-http",
                username="...", password="...")
tbl = conn.CASTable("kafka_stream", caslib="Public")

# 1. Row count should match the producer's "sent" (plus any at-least-once replays)
tbl.numRows()

# 2. Distribution of fraud patterns
tbl.groupby("fraud_pattern").summary(subSet=["N"])

# 3. Per-account counts of fraudulent rows; velocity bursts stand out
tbl[tbl.is_fraud == True].groupby("account_id").summary(subSet=["N"])

# 4. Geo-jumps: the same account in different countries within minutes
tbl[tbl.fraud_pattern == "geo_jump"].head(20)

```

To check for duplicate rows caused by at-least-once replays, list the Kafka positions that occur more than once:

```python
conn.loadactionset("fedSql")
conn.fedSql.execDirect(query="""
    select _kafka_partition, _kafka_offset, count(*) as copies
    from Public.kafka_stream
    group by _kafka_partition, _kafka_offset
    having count(*) > 1
""")
```

If rows with `is_fraud = true` are present and their count roughly matches the producer's `fraud_sent`, the pipeline is healthy.

## Adapting the generator to other domains

The pipeline downstream of the producer doesn't depend on the message shape, so it's straightforward to swap the generator for another domain. For a trading or order-book use case, for example:

```
order_id, event_time, instrument (AAPL, EUR/USD, BUND), side (BUY/SELL),
quantity, price, venue, trader_id, account_id, order_type (LIMIT/MARKET),
time_in_force (DAY/IOC/GTC), is_anomaly (spoofing / layering label)
```

Keep the same partitioning approach (key by `instrument`) and the same burst-injection idea, but replace the fraud patterns with patterns like **spoofing** (a large quote followed by a cancel) or **wash trading** (matched buys and sells between related accounts).
