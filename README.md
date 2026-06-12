# mandala-reindeer_x

> **This repository has moved.**
> Canonical home: [github.com/uvalib/mandala-reindeer_x](https://github.com/uvalib/mandala-reindeer_x)
> The legacy name `kmaps-solr-sync` and the `shanti-uva` org are preserved here for reference only.

---

Synchronization service for the [Mandala Digital Library](https://github.com/uvalib/mandala-navina)
platform at the University of Virginia Library. Runs as the `reindeer_x` Docker
container.

## What it does

Generates and maintains the **kmterms-in-kmassets shadow entries** — one kmasset
document per kmterm for the `subjects`, `places`, and `terms` asset types. This
makes KMaps taxonomy terms discoverable alongside content assets in a single Solr
index, allowing Drupal and the React front-end to treat subjects, places, and terms
as first-class assets without querying a separate index.

Also contains `synch` and `synchandler` — the scripts that watch Drupal's
`private/files/solrdocs/` directories and upload content asset documents to S3
for the ECS ingest pipeline. (These are candidates for consolidation into the
Node.js service — see [Spike 8](https://github.com/uvalib/mandala-navina/blob/main/docs/spikes/spike-08-reindeer-x-consolidation.md).)

## Name

`reindeer_x` is a pun: **re-index-er**, with the `x` displaced from `index`.

## Heritage

Originally developed as `kmaps-solr-sync` in the
[shanti-uva](https://github.com/shanti-uva/kmaps-solr-sync) GitHub organization.
Transferred to `uvalib/mandala-reindeer_x` in June 2026 to bring it under the
UVA Library deployment infrastructure alongside the rest of the Mandala platform.

## Architecture

```
KMaps Rails → S3 (kmterms-inbound)
  → SQS → ECS transform → ECS kmterms Solr update
    → [planned: SQS completion notification]
      → reindeer_x queries kmterms Solr
        → writes kmassets (subjects/places/terms)
```

The current trigger mechanism is a UDP ping from KMaps Rails (port 9001, default).
An HTTP endpoint (`POST /post`) is also available. Migration to SQS event-driven
triggering is planned — see Spike 8 linked above.

## Quick Start

```bash
cp .env.dist .env   # customize for your environment
npm run rdx-build-up
```

- Queue UI: http://localhost:9000 (default)
- Trigger a reindex: `echo | nc -w 0 -u localhost 9001`
- HTTP trigger: `curl -X POST http://localhost:9000/post -H "Content-Type: application/json" -d '{"query":"!schema_version_i:41"}'`

## Configuration

Copy `.env.dist` to `.env` and set:

| Variable | Description |
|---|---|
| `KMTERMS_UNAUTH_HOST` | kmterms Solr host (read-only) |
| `KMASSETS_AUTH_HOST` | kmassets Solr host (write) |
| `KMASSETS_AUTH_USER` / `_PASS` | kmassets Solr credentials |
| `MANDALA_BASE_URL` | Base URL for generated `url_html` fields |
| `DEFAULT_QUERY` | Solr filter query for changed kmterms |
| `REDIS_PASS` | Redis password (must match `docker/redis/redis.conf`) |
| `HTTP_PORT` / `UDP_PORT` | Service ports |

## Related

- [uvalib/mandala-navina](https://github.com/uvalib/mandala-navina) — main Mandala monorepo
- [docs/deferred/solr-sync-architecture-d11.md](https://github.com/uvalib/mandala-navina/blob/main/docs/deferred/solr-sync-architecture-d11.md) — D11 architecture context
- [docs/spikes/spike-08-reindeer-x-consolidation.md](https://github.com/uvalib/mandala-navina/blob/main/docs/spikes/spike-08-reindeer-x-consolidation.md) — planned modernization
