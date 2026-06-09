# Decision 0001: Artifact URLs are S3-only; local store uses authenticated download

- **status:** accepted
- **date:** 2026-06
- **related items:** `0001-artifact-store`, `0002-slack-adhoc-profiling`, `0003-results-portal`

## Decision

`ArtifactStore::signed_url(key, ttl)` is an **S3-mode capability only**.
`LocalFsStore::signed_url` returns **`Unsupported`**. Any consumer that needs to
hand a user a fetchable artifact (Slack links, the portal) must fall back to an
**orchestrator/portal-authenticated download endpoint** that streams the bytes
via `ArtifactStore::get`. No caller may assume a shareable URL exists under
`kind = "local"`.

## Rationale

A local filesystem can't mint an externally-usable URL. Putting `signed_url` on
the trait without this rule would let `0002`/`0003` silently assume links work in
local mode and break at runtime. Making the local case an explicit `Unsupported`
forces every consumer to carry the authenticated-download fallback, so both modes
are correct by construction.

## Consequences

- Slack (`0002`) and the portal (`0003`) implement the download-endpoint fallback;
  signed links are an S3 affordance layered on top, not a baseline assumption.
- `[artifacts].kind` drives the behavior; the trait surface is identical either
  way.
- The download endpoint inherits the consumer's own authorization (e.g. the
  portal's visibility scoping), so a local-mode fetch is never an auth bypass.
