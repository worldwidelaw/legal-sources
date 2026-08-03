# Queensland Legislation API (Official REST API)

**Source:** [https://www.legislation.qld.gov.au/api](https://www.legislation.qld.gov.au/api)
**API base:** `https://api.legislation.qld.gov.au`
**Country:** AU (jurisdiction AU-QLD)
**Data types:** legislation
**Status:** Blocked — code complete, awaiting durable account credentials

## What this is

The official Office of the Queensland Parliamentary Counsel (OQPC) REST API
for Queensland Acts and subordinate legislation. It returns structured QuILLS
DTD XML full text and supports point-in-time versions. Content is the same
corpus as the unauthenticated `AU/QLD-Legislation` source but delivered through
the official API (richer metadata, cleaner incremental updates).

## Authentication (runtime token exchange — never a stored JWT)

The API requires a free account. **Only the account credentials are durable
secrets**; access/refresh tokens are minted per run and never persisted or
logged.

| Endpoint | Body | Returns |
|----------|------|---------|
| `POST /v1/auth/token`   | `{ username, password }` | access + refresh tokens |
| `POST /v1/auth/refresh` | `{ refresh_token }`      | new access token |

Data calls send `Authorization: Bearer <access token>`.

Runtime flow implemented in `bootstrap.py` (`QLDApiClient`):

1. On first call, log in from `QLD_LEGISLATION_USERNAME` / `QLD_LEGISLATION_PASSWORD`.
2. Reuse the access token until ~30s before expiry.
3. Near expiry (or on a `401`), call `/v1/auth/refresh`.
4. If the refresh is rejected/expired, fall back to a full username/password
   re-login.
5. Tolerates both rotating (new refresh returned) and non-rotating refresh
   responses.

## Discovery + full text

- `GET /v1/documents?page=N&limit=50` — paginated document list (`_meta.total_pages`).
- `GET /v1/renditions/xml/{id}` — QuILLS DTD XML full text (tags stripped to plain text).

## Configuration

Set durable secrets (injected on the fleet via the standard `SOURCE_ENV_B64`
path — never committed):

```bash
export QLD_LEGISLATION_USERNAME="..."
export QLD_LEGISLATION_PASSWORD="..."
```

Register for an account: https://api.legislation.qld.gov.au/api/signup

Membership requires an email reconfirmation roughly every three months to
remain active; if auth begins returning `401` after a full re-login, the
account likely needs reconfirmation.

## Usage

```bash
python bootstrap.py test               # Auth + one document (needs creds)
python bootstrap.py bootstrap --sample # 15 sample records
python bootstrap.py bootstrap --full   # Full initial pull
python bootstrap.py bootstrap-fast     # Concurrent full pull
python bootstrap.py update             # Recently published documents
python test_auth.py                    # Auth/refresh/401 regression tests (no network)
```

`test` exits `2` when credentials are absent, `1` on auth failure.

## Tests

`test_auth.py` stubs the HTTP transport (no network, no real credentials) and
covers: token exchange, access-token reuse, rotating/non-rotating refresh,
refresh-rejection re-login, `401` recovery on a data call, missing-credentials
handling, and QuILLS full-text extraction + normalized record shape.

## Why still "blocked"

The scraper and auth flow are complete and unit-tested, but a live sample run
(10+ full-text records) requires an OQPC account. Once
`QLD_LEGISLATION_USERNAME` / `QLD_LEGISLATION_PASSWORD` are provisioned, the
fleet can run `bootstrap --full` and the source can be flipped to `complete`.

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — attribution required.
Queensland Government legislation is licensed CC BY 4.0.
