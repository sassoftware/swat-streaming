---
sidebar_position: 5
title: Authentication
description: Kafka and CAS credentials, delivering secrets to the pod, rotation, and a hardening checklist.
---

# Authentication

Streaming from Kafka into CAS crosses two trust boundaries, and each one needs its own credentials. Neither set should ever end up in a container image, a Git repository, or shell history.

| Hop | Recommended | Configured with |
|---|---|---|
| Kafka | SASL/SCRAM-SHA-512 over TLS | `kafka-credentials` Secret (username, password, CA) |
| CAS | OAuth bearer token from SAS Logon Manager | `cas-credentials` Secret (token, CA bundle) |

mTLS for Kafka and username/password for CAS are also supported.

## Kafka authentication

### Recommended: SASL/SCRAM-SHA-512 over TLS

This is the app's default. It gives you:

- transport encryption (TLS 1.2+)
- a password that is never sent over the wire (SCRAM uses challenge/response)
- per-user credentials that you can revoke without restarting brokers

**On the Kafka cluster** (one-time, by your Kafka administrator):

```bash
# Create a SCRAM user
kafka-configs.sh --bootstrap-server kafka:9093 \
  --alter --add-config 'SCRAM-SHA-512=[password=REPLACE_ME]' \
  --entity-type users --entity-name svc-viya-consumer

# Grant read on the topic and the consumer group
kafka-acls.sh --bootstrap-server kafka:9093 \
  --add --allow-principal User:svc-viya-consumer \
  --operation Read --operation Describe \
  --topic viya.events --group swat-streaming-viya-events
```

**In the pod**, provide the credentials through the mounted `kafka-credentials` Secret:

| Mount path | Secret key | Used as |
|---|---|---|
| `/etc/kafka/credentials/username` | `username` | SASL username (`KAFKA_SASL_USERNAME_FILE`) |
| `/etc/kafka/credentials/password` | `password` | SASL password (`KAFKA_SASL_PASSWORD_FILE`) |
| `/etc/kafka/certs/ca.crt` | `ca.crt` | Trust anchor for the broker certificate (`KAFKA_SSL_CA_LOCATION`) |

### Alternative: mTLS (client certificates)

If your Kafka cluster authenticates clients by certificate, set `KAFKA_SECURITY_PROTOCOL=SSL` and mount:

| Variable | File mounted from a Secret |
|---|---|
| `KAFKA_SSL_CA_LOCATION` | `/etc/kafka/certs/ca.crt` |
| `KAFKA_SSL_CERTIFICATE_LOCATION` | `/etc/kafka/certs/client.crt` |
| `KAFKA_SSL_KEY_LOCATION` | `/etc/kafka/certs/client.key` |

Remove the SASL username/password mount. The principal is the subject of the client certificate, so grant ACLs against that DN.

mTLS is operationally harder than SCRAM (certificate rotation, CRL management), but it's the better fit in environments that already standardize on PKI.

### Producer credentials

The [synthetic producer](testing.md) needs different permissions than the consumer. Provision a **separate** Kafka principal and Secret for it rather than reusing the consumer's:

```bash
# On the Kafka cluster: producer-only ACL
kafka-acls.sh --bootstrap-server kafka:9093 \
  --add --allow-principal User:svc-viya-producer \
  --operation Write --operation Describe \
  --topic viya.events

# In the cluster: a separate Secret
kubectl -n swat-streaming create secret generic kafka-producer-credentials \
  --from-literal=username='svc-viya-producer' \
  --from-literal=password='REPLACE_ME' \
  --from-file=ca.crt=./kafka-ca.crt
```

Then point the volumes in `k8s/producer-job.yaml` at `kafka-producer-credentials` instead of `kafka-credentials`. With split principals, the consumer stays read-only: a compromised consumer pod can't inject messages, and a compromised producer pod can't read messages or commit offsets.

### Discouraged options

- `SASL_PLAINTEXT` sends credentials over the network unencrypted.
- `PLAINTEXT` has no authentication and no encryption. Use it only in a lab.
- `SASL/PLAIN` over TLS works, but SCRAM is strictly better.

## CAS authentication

The container talks to CAS through SWAT's HTTPS (REST) protocol. These are the options, in order of preference.

### Recommended: OAuth token from SAS Logon Manager

SAS Logon Manager issues OAuth2 access tokens, each bound to a registered client and usually to a service user. SWAT accepts the token as its `password` argument over HTTPS.

**One-time setup (Viya administrator).** Register a client that can obtain tokens for the service user, for example with `sas-admin`:

```bash
sas-admin auth login
sas-admin oauthclient create \
  --client-id swat-streaming \
  --client-secret 'REPLACE_ME' \
  --scope 'openid' \
  --grant-type password \
  --access-token-validity 43200 \
  --refresh-token-validity 2592000
```

Grant the client only the scopes it needs. Don't give the streaming client administrative scopes.

**Get a token.** Do this outside the cluster, for example in CI or your secret manager, and store the resulting `access_token` in the `cas-credentials` Secret under the key `token`:

```bash
ACCESS_TOKEN=$(curl -sS -u "swat-streaming:REPLACE_ME" \
  -d 'grant_type=password' \
  -d 'username=svc-viya-streamer' \
  -d 'password=REPLACE_ME' \
  https://viya.example.com/SASLogon/oauth/token | jq -r .access_token)
```

The app reads the token from `CAS_TOKEN_FILE=/etc/cas/credentials/token`.

:::warning Token expiry
Viya access tokens are short-lived (12 hours by default), and the app reads the token **only once, at startup**. For long-running pods, use one of these:

- a sidecar that refreshes the token with the refresh token and writes it to a shared volume, combined with periodic pod restarts
- External Secrets Operator with a generator that calls SAS Logon, plus a rollout when the Secret changes
- a Vault Agent template that writes a fresh token to disk

If none of these are available, set the access-token validity long enough to cover the pod's lifetime and rotate with a scheduled redeploy.
:::

### Username and password

If OAuth isn't available, set `CAS_USERNAME_FILE` and `CAS_PASSWORD_FILE` instead of `CAS_TOKEN_FILE`. Always use a **dedicated service user** from your identity provider, never a personal account.

### Kerberos

Kerberos is possible but heavy to operate in containers (keytab management, clock skew, KDC reachability). If you must use it, mount the keytab from a Secret, install `krb5-user` in the image, and provide `/etc/krb5.conf` through a ConfigMap. Treat it as a last resort.

### CAS authorization

Authentication only proves who the caller is. The service user still needs authorization in CAS. On the **target caslib**, grant:

- `ReadInfo`, `Select`, `CreateTable`, `LimitedPromote`, and `Promote` (only if `CAS_TABLE_PROMOTE=true`)

Grant nothing more. The service user shouldn't have administrative privileges. In SAS Environment Manager, these settings are under *Data → Libraries → (your caslib) → Authorization*.

## Secret delivery into the pod

Whichever tool you use, the contract with the app is the same: credentials show up as **files** under `/etc/kafka/credentials/` and `/etc/cas/credentials/`, the `*_FILE` environment variables point at those files, and the app reads them at startup. Plain environment-variable secrets also work but are discouraged, because they appear in `kubectl describe pod` output.

### Plain Kubernetes Secrets

Kubernetes Secrets are only base64-encoded at rest by default. Before relying on them, check that:

- etcd encryption at rest is enabled, with a KMS provider
- RBAC limits `get` / `list` on Secrets in the `swat-streaming` namespace to a small set of principals
- audit logging records Secret reads

### Recommended for production

| Tool | Why |
|---|---|
| **External Secrets Operator** | Syncs secrets from AWS Secrets Manager, GCP Secret Manager, Azure Key Vault, or Vault into Kubernetes Secrets. Your enterprise secret store stays the source of truth. |
| **Sealed Secrets** | Encrypts Secret manifests with a key that only the cluster can decrypt, so the sealed manifests are safe to commit to Git. |
| **Vault Agent Injector** | Writes secrets straight into the pod as files, without ever creating a `Secret` object. |
| **Secrets Store CSI Driver** | Mounts secrets from an external store as files through CSI, with native rotation support. |

### Rotation

The app doesn't re-read credentials while it runs. After rotating any credential, update the Secret and then restart the Deployment:

```bash
kubectl -n swat-streaming rollout restart deploy/swat-streaming
```

| Credential | Procedure |
|---|---|
| Kafka SCRAM password | Change it on the broker → update the Secret → restart |
| Kafka CA | Roll out the new CA in the Secret → restart |
| CAS OAuth token | Automated refresh (see [token expiry](#recommended-oauth-token-from-sas-logon-manager)) → restart |
| CAS password | Rotate in the identity provider → update the Secret → restart |

## Hardening checklist

- [ ] etcd encryption at rest is enabled with a KMS provider.
- [ ] The `NetworkPolicy` (`k8s/networkpolicy.yaml`) allows egress only to the Kafka brokers and the Viya CAS endpoint, with the CIDRs narrowed to your networks.
- [ ] The pod runs as non-root (`runAsNonRoot: true`, UID 10001).
- [ ] The pod has `readOnlyRootFilesystem: true` and drops all capabilities.
- [ ] Secrets are mounted as files with mode `0400`, not passed as environment variables.
- [ ] `automountServiceAccountToken: false` is set. The app never calls the Kubernetes API.
- [ ] The Kafka **consumer** principal has read-only ACLs on exactly one topic.
- [ ] If you run the synthetic producer, it uses a **separate** principal with only Write + Describe on the topic.
- [ ] The CAS service user has the minimum caslib permissions for the target table.
- [ ] OAuth tokens are short-lived and rotated automatically.
- [ ] The image is signed (for example with cosign) and pinned by digest in the Deployment.
- [ ] Logs contain no credentials. The app only logs `auth: token` or `auth: password`, never the value.
