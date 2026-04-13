# US/LegiScan — US State + Congress Legislation

[LegiScan](https://legiscan.com/) is a comprehensive tracker of US state and
federal legislation. Their JSON API (`api.legiscan.com`) covers all 50 states
+ DC + US Congress with full bill text, sponsors, roll calls, amendments,
and historical sessions.

## Coverage

- All 50 US states + DC + US Congress (52 jurisdictions)
- Full bill text (PDF and/or HTML) via `getBillText`
- Sponsors, committees, subjects, status history
- Historical sessions (not just current)

## Authentication

Free tier: **30,000 queries/month**.

Register at https://legiscan.com/legiscan-register and copy `.env.template` to
`.env`, then fill in `LEGISCAN_API_KEY=...`.

**ToS:** free tier is intended for non-commercial / public-benefit use.
Commercial use requires a paid GAITS subscription.

## Ingest strategy

The scraper uses the `getSessionList` → `getDataset` flow:

1. `getSessionList` per state (52 calls) returns every session ever held.
2. `getDataset` per session (~150 calls total to backfill historically) returns
   a base64-encoded ZIP containing every bill in that session as JSON.
3. Each bill is parsed in-memory; the most recent text version is fetched via
   `getBillText`. PDF versions are routed through `common/pdf_extract.py` for
   markdown extraction.

Total quota for a full historical backfill: **~150 queries**, leaving plenty
of headroom for incremental updates.

## Commands

```bash
# Sample run (10 records)
python3 sources/US/LegiScan/bootstrap.py bootstrap --sample

# Full backfill
python3 sources/US/LegiScan/bootstrap.py bootstrap

# Incremental update
python3 sources/US/LegiScan/bootstrap.py update
```

## Status

Scaffold is in place but **awaiting LEGISCAN_API_KEY registration** by the
admin before first run. Once the key is in `.env`, run the sample command
above to validate.
