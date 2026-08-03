# Superseded v22 Plan: Multi-Variant Benchmark Comparisons

v22 implemented two-variant request validation, atomic multi-spec persistence,
serial execution, per-variant calibration, shared artifact carry-forward,
comparison aggregation, and one aggregate Slack report. Its remaining work was
real-host validation.

The original plan predated the fleet scheduler and treated deterministic Slack
flags as a supported intake. The current architecture uses pull scheduling,
canonical snapshot reporting, and LLM-only conversational Slack intake; typed
HTTP/CLI and exact GitHub triggers remain provider-free.

The current `0039-multi-variant-benchmark-comparisons` item and its relevant
hardware/integration acceptance moved to
[v34](../../iterations/v34-first-fleet-deployment-readiness.md). v34 validates
the current production path rather than the retired syntax or scheduling
assumptions.

