# Decision 0004: Protobuf/gRPC Worker Fleet Protocol

- **status:** accepted
- **date:** 2026-07
- **related items:** `0004-worker-fleet`, `0017-generic-phase-events`,
  `0074-protobuf-fleet-protocol`,
  `0075-rolling-worker-protocol-compatibility`

## Decision

The daemon/worker control plane uses a versioned Protocol Buffers schema and
gRPC over HTTP/2. The workspace `proto/` tree is authoritative. `sbgh-proto`
owns generated Rust messages, client/server stubs, wire conversion, and typed
gRPC status details. `sbgh-fleet` owns dependency-light transport-neutral
values, validation, and semantic digests. Daemon and worker transport adapters
convert generated messages before application logic; generated wire messages
do not become persistence rows, execution backend types, or reporting models.

The worker continues to initiate every control-plane operation. Polling remains
a bounded unary long-poll; adopting gRPC does not authorize daemon-pushed work.
Presigned artifact bytes continue to move directly over HTTPS.

The initial protobuf fleet uses protocol revision 1 and requires an exact
revision match at session registration. The accepted revision is persisted on
the session; later RPCs bind that session or a fenced attempt instead of
repeating a version field. Worker software version is separate telemetry and
does not require a protocol bump when wire fields and semantics are unchanged.
Supporting simultaneous protocol revisions and rolling protocol upgrades is a
separate follow-up (`0075`) required before the first incompatible protobuf
change.

TLS 1.3 mutual X.509 authentication, certificate URI-SAN worker identity,
server-owned capability authorization, message validation/bounds, attempt
leases, fencing, idempotency, and cleanup remain mandatory. Protobuf decoding
is not authorization or validation.

Payload and terminal identities remain semantic digests. They do not hash
serialized protobuf bytes because protobuf serialization is not canonical
across schema and implementation changes.

## Rationale

Protocol Buffers gives the fleet an explicit language-neutral schema and
generated client/server interface. gRPC supplies standard RPC framing, status,
deadlines, and HTTP/2 transport while preserving the existing pull-based
application state machine.

The immediate deployment has one central operator and one worker, with a second
operator installing the same current release. No JSON fleet has been deployed,
so preserving its field layout, digest constants, revision number, or stored
messages would add machinery without a compatibility target. The first
deployed protobuf schema becomes the concrete input to later compatibility
work.

## Consequences

- v29 removes the undeployed JSON/HTTP prototype and makes protobuf/gRPC the
  first deployed worker transport.
- Schema linting and reproducible code generation are required immediately.
  Breaking-change enforcement against a previous release begins with `0075`.
- Exact protocol mismatch fails before session registration or assignment.
  Ordinary software releases need not change the protocol version.
- mTLS identities, registry authorization, leases, events, and
  artifact-transfer policy remain mandatory.
- Generated messages remain at transport boundaries, keeping `sbgh-core`,
  `sbgh-driver`, and `sbgh-postgres` independent of Prost, Tonic, and gRPC.
- `sbgh-proto` now means protobuf rather than an ambiguous handwritten JSON
  protocol crate.
