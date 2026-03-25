# Legal Data Hunter

Comprehensive, evergreen database of laws, regulations, and case law from all 27 EU member states — built through systematic, automated scraping of open data sources.

## What This Is

A monorepo containing ~130 individual scrapers (and growing), each targeting a specific legal data source in an EU country. Every scraper follows the same interface and can bootstrap a full dataset or incrementally update it.

## Architecture

```
legal-data-hunter/
  manifest.yaml          ← Master inventory: all sources + status
  INBOX.md               ← Admin → Claude: messages and requests
  BLOCKED.md             ← Claude → Admin: things needing human action
  runner.py              ← Session entry point: picks next task
  common/                ← Shared libraries
    base_scraper.py        Base class all scrapers inherit from
    http_client.py         HTTP client with retries + caching
    storage.py             JSONL-based storage with dedup
    rate_limiter.py        Token bucket rate limiter
    validators.py          Schema validation
  templates/             ← Templates for new sources
    config_template.yaml   Config file template
    scraper_template.py    Scraper code template
  sources/               ← One directory per data source
    {COUNTRY_CODE}/
      {source_name}/
        config.yaml        Source configuration
        bootstrap.py       Scraper implementation
        status.yaml        Run history and stats
        sample/            Sample data (10+ documents)
        README.md          Source documentation
        .env.template      Required secrets (if any)
```

## How It Works

### Per-Source Interface

Every scraper implements three methods:
- `fetch_all()` — yields all documents (for bootstrap)
- `fetch_updates(since)` — yields documents modified since a date
- `normalize(raw)` — transforms raw data into standard schema

### Two Data Models

**Legislation** (mutable): Laws get amended. Same ID, new content. Strategy: **upsert** with version tracking.

**Case law** (immutable): Court decisions don't change after publication. Strategy: **append-only** with dedup.

### Session Workflow

Each automated session:
1. Reads `INBOX.md` for admin messages
2. Reads `manifest.yaml` for next task
3. Builds the scraper, tests with sample data
4. Commits and pushes
5. Moves to next source or stops

## Running

```bash
# Check project status
python runner.py status

# Find and display the next source to build
python runner.py next

# Test a specific source's scraper (sample mode)
python runner.py sample FR/legifrance

# Run a full bootstrap for a source
python runner.py test FR/legifrance
```

## For the Admin (Zach)

### Providing Secrets
When a source needs an API key:
1. Check `BLOCKED.md` for instructions
2. Register at the URL provided
3. Add the key to `sources/{country}/{source}/.env`
4. Or paste it in chat during a Cowork session

### Flagging Issues
Write in `INBOX.md` under today's date:
```markdown
## 2026-02-09
- The ES/boe scraper is broken, they changed the API. Please investigate.
- I've added the PL/sejm_api key. Unblock it.
- Prioritize Italian sources next.
```

### Reviewing Data
After a scraper is built, check `sources/{country}/{source}/sample/` for 10+ sample documents with all fields. The `config.yaml` flags which fields are likely most important.

## Status

Track progress in `manifest.yaml`. Each source has a status:
- `planned` — Not started
- `in_progress` — Currently being built
- `blocked` — Needs admin action (see BLOCKED.md)
- `review` — Built, needs admin review
- `complete` — Working and tested
