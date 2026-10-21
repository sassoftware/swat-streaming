# Testing with the synthetic producer

The `producer/` package ships a Kafka producer that generates a realistic
stream of card-payment transactions for end-to-end testing of the
streaming pipeline. It is intentionally shaped around a **fraud-detection**
use case: stateful card/account population, mostly-legit traffic with
configurable bursts of known fraud patterns, geo-aware merchants, MCCs, and
multiple payment channels.

The producer reuses the same container image and the same Kafka auth wiring
as the consumer — only the entrypoint differs.

---

## 1. Message schema

Every record is a JSON object keyed by `account_id` (so all transactions for
one card land on the same Kafka partition and preserve order).

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

Notes:
- `card_id` digits pass the Luhn check but are entirely synthetic.
- `device_id` and `ip_address` are populated only for `channel = "online"`.
- `is_fraud` / `fraud_pattern` are ground-truth labels you can train against
  or use to assert end-to-end correctness — the real world does not deliver
  these, of course.

---

## 2. Fraud patterns

When a burst fires (probability per emission = `PRODUCER_FRAUD_RATE`), the
generator picks one of five patterns and emits all messages of the burst
back-to-back:

| Pattern | Shape | Typical signal |
|---|---|---|
| `velocity` | 6–12 small online charges on one card within ~30 s | Rule: N txns / T seconds on same card |
| `geo_jump` | Domestic txn followed by a foreign-country txn minutes later | Geo-impossibility check |
| `high_amount` | One charge 25–80× the account's typical amount | Z-score on amount distribution |
| `card_testing` | 4–9 micro-charges across varied merchants | Amount + merchant entropy |
| `cnp_foreign` | Single online transaction from a high-risk country | Channel + country score |

These are deliberately easy to detect — the point is to validate the
**pipeline** (Kafka → CAS), not your model.

---

## 3. Running the producer

### 3.1 As a Kubernetes Job (recommended)

```bash
kubectl apply -f k8s/producer-job.yaml
kubectl -n swat-streaming logs -f job/swat-streaming-producer
```

The Job defaults to **50 msg/s for 10 minutes** with **2% fraud rate**.
Edit the `env:` block in `k8s/producer-job.yaml` to change duration, rate,
or fraud injection frequency. The Job ships with `restartPolicy: Never` and
`backoffLimit: 0` so a configuration error fails fast rather than retrying.

**Credentials**: the example Job reuses the `kafka-credentials` Secret. In
real environments grant the consumer service user **read-only** ACL and
create a separate `kafka-producer-credentials` Secret with a producer-only
user that has Write+Describe on the topic — then point the Job's volumes at
the producer secret instead.

### 3.2 Locally (against a dev Kafka)

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

(`KAFKA_GROUP_ID` is required by the shared config dataclass even though the
producer ignores it.)

### 3.3 Tunables

| Variable | Default | Purpose |
|---|---|---|
| `PRODUCER_RATE_PER_SECOND` | `50` | Target send rate (best-effort, no burst spike) |
| `PRODUCER_DURATION_SECONDS` | unset | Stop after N seconds (omit to run until interrupted or `PRODUCER_TOTAL_MESSAGES`) |
| `PRODUCER_TOTAL_MESSAGES` | unset | Stop after N messages |
| `PRODUCER_NUM_ACCOUNTS` | `5000` | Size of the card/account pool |
| `PRODUCER_NUM_MERCHANTS` | `800` | Size of the merchant pool |
| `PRODUCER_FRAUD_RATE` | `0.01` | Probability that a given emission triggers a fraud burst |
| `PRODUCER_SEED` | unset | Seed the PRNG for reproducible runs |
| `PRODUCER_FLUSH_EVERY` | `500` | Log a progress line every N messages |

---

## 4. Verifying end-to-end in CAS

After running the producer, the consumer should have written rows to
`<CAS_CASLIB>.<CAS_TABLE_NAME>`. Quick checks from a SWAT session:

```python
import swat
conn = swat.CAS("https://viya.example.com/cas-shared-default-http",
                username="...", password="...")
tbl = conn.CASTable("kafka_stream", caslib="Public")

# 1. Row count matches what the producer reported (modulo at-least-once replays)
tbl.numRows()

# 2. Fraud-pattern distribution
tbl.groupby("fraud_pattern").summary(subSet=["N"])

# 3. Per-account velocity — should highlight the injected `velocity` bursts
tbl[tbl.is_fraud == True].groupby("account_id").summary(subSet=["N"])

# 4. Geo-jumps: same account, distinct countries within a short window
tbl[tbl.fraud_pattern == "geo_jump"].head(20)
```

If `is_fraud=true` rows are present and the counts roughly match the
producer's `fraud_sent` log line, the pipeline is healthy.

---

## 5. Trading-data variant

For an order-book / trade-execution use case rather than card fraud, swap
the generator's field set:

```
order_id, event_time, instrument (AAPL, EUR/USD, BUND), side (BUY/SELL),
quantity, price, venue, trader_id, account_id, order_type (LIMIT/MARKET),
time_in_force (DAY/IOC/GTC), is_anomaly (spoofing / layering label)
```

Keep the same partitioning strategy (key by `instrument`) and the same
burst-injection idea, but swap the fraud patterns for things like
**spoofing** (large quote then cancel) or **wash trading** (matched buy/sell
between related accounts). The pipeline downstream of the producer does not
change.
