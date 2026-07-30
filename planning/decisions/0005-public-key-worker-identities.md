# Decision 0005: Public-Key Worker Identities

- **status:** accepted
- **date:** 2026-07
- **related items:** `0004-worker-fleet`,
  `0076-database-backed-worker-registry`,
  `0077-worker-identity-and-config-simplification`

## Decision

The centralized daemon authenticates to workers with a normal publicly trusted
TLS server certificate for its configured DNS name. Certificate issuance and
renewal are external deployment concerns; the daemon loads the resulting
certificate chain and private key. Workers use the platform trust store and do
not configure a daemon CA file, pinned daemon key, trust-on-first-use, or an
insecure verification mode.

Each worker owns one application identity private key. Its public key is
enrolled through the daemon's authenticated admin API and stored as a
SHA-256 digest of canonical SubjectPublicKeyInfo (SPKI). The TLS client
certificate required by the HTTP/2 stack is a short-lived, self-signed wrapper
generated from that key by the worker. It is transport plumbing rather than an
operator-managed credential or identity record.

The initial identity format is ECDSA P-256: unencrypted PKCS#8 PEM for the
private key and SPKI PEM for enrollment. This matches the existing TLS stack
and keeps one algorithm and canonical encoding in the first deployed contract.
Algorithm agility may be added when a second form is actually required.

During a connection, TLS proves possession of the private key. The daemon
derives the SPKI digest from the authenticated peer and resolves it to a
server-owned worker registry record before any RPC may create or mutate state.
Every RPC rechecks current database authorization. Certificate issuer, Common
Name, URI SAN, request fields, and certificate fingerprint are not identity
fallbacks.

The daemon-generated worker UUID remains the stable resource, policy,
placement, and history identity. Public keys are rotatable credentials:

```text
worker UUID
  ├── active public-key fingerprint A
  └── active public-key fingerprint B
```

Overlapping enrollment permits rotation. Revoked fingerprints remain
auditable and may never be resurrected or reassigned.

## Rationale

The prior private-CA design required every worker operator to receive and
configure a client certificate, private key, daemon CA certificate, and worker
UUID. That ceremony duplicated an identity already authenticated by key
possession and made the central operator an online certificate-issuance
bottleneck.

Public-key enrollment provides the identity model used by systems such as SSH
and libp2p while retaining TLS 1.3 and gRPC. Public Web PKI solves the distinct
daemon-authentication problem once, centrally. Keeping the logical worker UUID
separate from its keys preserves history and policy across rotation.

## Consequences

- Worker configuration needs only the orchestrator URL and identity private-key
  path for control-plane authentication.
- The daemon's public TLS certificate must remain valid for the configured
  hostname. Renewal may restart or reload the daemon; workers reconnect through
  the existing session and fencing model.
- The TLS listener accepts a bounded, well-formed self-signed client wrapper
  far enough to prove key possession. Unknown or revoked keys are denied by
  database authorization before registration or other side effects.
- Handshake concurrency, certificate size, supported algorithm, validity,
  client usage, and chain-shape checks remain fail closed.
- Worker private keys never cross the admin API. Enrollment accepts only a
  canonical public key.
- There is no private worker CA, URI-SAN identity, certificate enrollment,
  silent TOFU, or `accept_invalid_certificates` mode.
- A future hardware-backed key or workload-identity mechanism may implement
  the same public-key identity contract without changing worker UUIDs or fleet
  application semantics.
