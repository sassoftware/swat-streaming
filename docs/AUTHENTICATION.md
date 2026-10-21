# Authentication Guide

Streaming from Kafka into CAS crosses two trust boundaries that each need
their own credentials. Both must be provisioned without ever landing in
container images, git repositories, or shell history.

This document covers:

1. [Kafka authentication](#1-kafka-authentication) — SASL/SCRAM over TLS (recommended), mTLS, alternatives
2. [SAS Viya / CAS authentication](#2-sas-viya--cas-authentication) — OAuth bearer tokens (recommended), username/password, dedicated service users
3. [Secret delivery into the pod](#3-secret-delivery-into-the-pod) — Kubernetes Secrets, External Secrets Operator, Sealed Secrets, Vault Agent Injector
4. [Hardening checklist](#4-hardening-checklist)

---

## 1. Kafka authentication

### 1.1 Recommended: SASL/SCRAM-SHA-512 over TLS

This is the default the app is configured for. It gives you:

- transport encryption (TLS 1.2+)
- a password that is never sent over the wire (SCRAM challenge/response)
- per-user credentials revocable from the Kafka cluster without recycling
  brokers

**On the Kafka cluster** (one-time, by your Kafka admin):

```bash
# Strimzi / Apache Kafka — create a SCRAM user
kafka-configs.sh --bootstrap-server kafka:9093 \
  --alter --add-config 'SCRAM-SHA-512=[password=REPLACE_ME]' \
  --entity-type users --entity-name svc-viya-consumer

# Grant read on the topic and consumer group
kafka-acls.sh --bootstrap-server kafka:9093 \
  --add --allow-principal User:svc-viya-consumer \
  --operation Read --operation Describe \
  --topic viya.events --group swat-streaming-viya-events
```

**In the pod** — provide these values via the mounted Kafka secret:

| Mount path | Source | Used as |
|---|---|---|
| `/etc/kafka/credentials/username` | secret key `username` | SASL username |
| `/etc/kafka/credentials/password` | secret key `password` | SASL password |
| `/etc/kafka/certs/ca.crt`         | secret key `ca.crt`   | Trust anchor for the broker certificate |

The app reads the credential files via `KAFKA_SASL_USERNAME_FILE` and
`KAFKA_SASL_PASSWORD_FILE` — credentials never live in environment variables,
which means they do not appear in `kubectl describe pod`, container inspect,
or process listings inside the container.

### 1.2 Alternative: mTLS (client certificates)

If your Kafka cluster authenticates clients by certificate, switch to
`KAFKA_SECURITY_PROTOCOL=SSL` and provide:

| Variable | File mounted from secret |
|---|---|
| `KAFKA_SSL_CA_LOCATION` | `/etc/kafka/certs/ca.crt` |
| `KAFKA_SSL_CERTIFICATE_LOCATION` | `/etc/kafka/certs/client.crt` |
| `KAFKA_SSL_KEY_LOCATION` | `/etc/kafka/certs/client.key` |

Drop the SASL username/password mount entirely. The principal is the
Subject of the client certificate; grant ACLs against that DN.

mTLS scales worse than SCRAM operationally (cert rotation, CRL management)
but is preferred in environments that already standardize on PKI.

### 1.3 Producer credentials (testing / synthetic load)

The synthetic producer shipped under `producer/` (see
[`TESTING.md`](TESTING.md)) requires a *different* set of permissions than
the consumer. Provision a **separate** Kafka principal and Secret rather
than reusing the consumer credentials:

```bash
# On the Kafka cluster — producer-only ACL
kafka-acls.sh --bootstrap-server kafka:9093 \
  --add --allow-principal User:svc-viya-producer \
  --operation Write --operation Describe \
  --topic viya.events

# In the cluster — separate Secret
kubectl -n swat-streaming create secret generic kafka-producer-credentials \
  --from-literal=username='svc-viya-producer' \
  --from-literal=password='REPLACE_ME' \
  --from-file=ca.crt=./kafka-ca.crt
```

Then point `k8s/producer-job.yaml`'s volumes at `kafka-producer-credentials`
instead of `kafka-credentials`. Splitting the principals means the consumer
remains read-only on the topic — a compromised consumer pod cannot inject
messages, and a compromised producer pod cannot read or commit offsets.

### 1.4 Discouraged

- `SASL_PLAINTEXT` — credentials traverse the network unencrypted.
- `PLAINTEXT` — no auth, no encryption. Lab only.
- `SASL/PLAIN` over TLS — works, but SCRAM is strictly better (password
  never decryptable from a captured TLS trace if the session key is
  later compromised).

---

## 2. SAS Viya / CAS authentication

The container talks to CAS through SWAT's HTTPS (REST) protocol. Three
options, in order of preference:

### 2.1 Recommended: OAuth bearer token from SAS Logon Manager

Viya issues OAuth2 access tokens via SAS Logon Manager. A token is bound to
a registered client and (usually) a service user. SWAT accepts the token as
the `password` argument over HTTPS.

**One-time setup (Viya administrator)** — register a confidential client with
the `password` grant type, scoped to the service user. Using `sas-admin`:

```bash
sas-admin auth login
sas-admin oauthclient create \
  --client-id swat-streaming \
  --client-secret 'REPLACE_ME' \
  --scope 'openid SASAdministrators' \
  --grant-type password \
  --access-token-validity 43200 \
  --refresh-token-validity 2592000
```

**Token issuance** — perform out-of-band (in your CI/secret manager), then
push the resulting `access_token` into the `cas-credentials` secret under
key `token`:

```bash
ACCESS_TOKEN=$(curl -sS -u "swat-streaming:REPLACE_ME" \
  -d 'grant_type=password' \
  -d 'username=svc-viya-streamer' \
  -d 'password=REPLACE_ME' \
  https://viya.example.com/SASLogon/oauth/token | jq -r .access_token)
```

The app picks the token up via `CAS_TOKEN_FILE=/etc/cas/credentials/token`.

**Token expiry.** Viya access tokens are typically short-lived (12 h by
default). For long-running pods, use one of:

- A sidecar that refreshes the token (using the refresh token) and writes it
  to a shared `emptyDir` volume that the main container reads.
- External Secrets Operator with a custom generator that calls SAS Logon.
- Vault's `oauth2` plugin or Vault Agent template to materialize a fresh
  token on disk.

Failing all of those, set `access-token-validity` long enough to cover the
pod's lifetime and rotate via a scheduled redeploy.

### 2.2 Username / password

If OAuth is unavailable, configure `CAS_USERNAME_FILE` and
`CAS_PASSWORD_FILE` instead of `CAS_TOKEN_FILE`. SWAT will authenticate the
session against SAS Logon transparently. Use a **dedicated service user**
provisioned in your identity provider — never a human account.

### 2.3 Kerberos

Possible but operationally heavy in containers (keytab management,
clock skew, KDC reachability). If you must, mount the keytab via a secret,
install `krb5-user` in the image, and configure `/etc/krb5.conf` via a
ConfigMap. Treat this as a last resort.

### 2.4 CAS authorization

Authentication only proves who the caller is — the service user still needs
authorization in CAS. Grant:

- `ReadInfo`, `Select`, `LimitedPromote`, `Promote` (if `CAS_TABLE_PROMOTE=true`),
  and `CreateTable` on the **target caslib**.
- Nothing more. The service user should not have administrative privileges.

In SAS Environment Manager: *Data → Libraries → \<caslib\> → Authorization*.

---

## 3. Secret delivery into the pod

### 3.1 Plain Kubernetes Secrets (baseline)

Kubernetes Secrets are base64-encoded (not encrypted) at rest by default.
Before relying on them, confirm:

- etcd is encrypted at rest (`--encryption-provider-config` on the API
  server, with a KMS provider).
- RBAC restricts `get`/`list` on Secrets in the `swat-streaming` namespace
  to a small set of principals (`kubectl auth can-i list secrets -n swat-streaming`
  should return `no` for almost everyone).
- Audit logging captures Secret reads.

Apply secrets via `kubectl create secret` or out-of-band manifests — do not
check `Secret` manifests into git.

### 3.2 Recommended for production

| Tool | Why |
|---|---|
| **External Secrets Operator** | Sync secrets from AWS Secrets Manager / GCP Secret Manager / Azure Key Vault / Vault into Kubernetes Secrets. Source of truth stays in your enterprise secret store. |
| **Sealed Secrets (Bitnami)** | Encrypts Secret manifests with a cluster-only public key. Sealed manifests are safe to commit to git; the controller decrypts them in-cluster. |
| **HashiCorp Vault Agent Injector** | Injects secrets directly as files into pods without ever creating a `Secret` object. Best fit if Vault is already your secret store. |
| **CSI Secrets Store driver** | Mounts secrets from external KMS as files via the CSI interface. Native rotation support. |

Whichever you pick, the contract with this app is the same: credentials
appear as **files** under `/etc/kafka/credentials/*` and
`/etc/cas/credentials/*`, the file paths are passed to the app via the
`*_FILE` environment variables, and the app reads them at startup. Plain
env-var secrets are also supported but discouraged because they end up in
`kubectl describe pod` output.

### 3.3 Rotation

- **Kafka SCRAM password.** Update on the broker, then update the Secret.
  The next pod restart picks it up. (The app does not re-read credentials
  in-place — restart the Deployment after rotation.)
- **Kafka TLS CA.** Same — restart after rolling the Secret.
- **CAS OAuth token.** Either schedule a CronJob/External Secrets refresh
  that updates the Secret and triggers a rollout, or run a token-refresh
  sidecar (see §2.1).
- **CAS password.** Identity provider rotation → update Secret → rollout.

---

## 4. Hardening checklist

- [ ] etcd encryption at rest is enabled with a KMS provider.
- [ ] The `swat-streaming` namespace has a `NetworkPolicy` that allows
      egress only to Kafka brokers and the Viya CAS endpoint
      (see `k8s/networkpolicy.yaml`).
- [ ] The pod runs as non-root (`runAsNonRoot: true`, UID 10001).
- [ ] The pod has `readOnlyRootFilesystem: true` and drops all capabilities.
- [ ] Secrets are mounted as files with mode `0400`, not env vars.
- [ ] `automountServiceAccountToken: false` is set (the app does not call
      the Kubernetes API).
- [ ] The Kafka **consumer** service account has read-only ACL on exactly one topic.
- [ ] If running the synthetic producer: the **producer** uses a *separate*
      Kafka principal with Write+Describe (only) on the same topic, backed
      by its own Secret (`kafka-producer-credentials`).
- [ ] The CAS service user has the minimum CASLIB permissions for the
      target table.
- [ ] OAuth tokens are short-lived and rotated by an automated mechanism.
- [ ] Image is signed (cosign) and pinned by digest in the Deployment.
- [ ] Container logs do not contain credentials. (The app intentionally
      logs only `auth: token` / `auth: password` — never the value.)
