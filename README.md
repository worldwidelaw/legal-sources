# World Wide Law

**Open-source legal data scrapers for 50+ countries.**

Every country publishes its laws, court decisions, and regulations online -- but in different formats, behind different APIs, with different access rules. World Wide Law is building the open infrastructure to collect, normalize, and make all of it accessible.

## What's Here

This repository contains **227 data source definitions** and **188 working scrapers** that download and normalize legal data from government portals worldwide. Each scraper follows a standard interface so that any developer can run, test, or improve it.

```
sources/
  FR/LegifranceCodes/     # French consolidated legal codes
  DE/GesetzeImInternet/   # German federal laws
  IT/NormattivaLegislation/ # Italian legislation
  ES/BOE/                 # Spanish official gazette
  ... (50+ countries)
```

## Quick Start

```bash
# Clone the repo
git clone https://github.com/worldwidelaw/legal-sources.git
cd legal-sources

# Install dependencies
pip install -r requirements.txt

# Check project status
python runner.py status

# Test a specific source
python runner.py sample FR/LegifranceCodes

# See what needs work
python runner.py next
```

## How It Works

### Per-Source Structure

Every source lives in `sources/{COUNTRY_CODE}/{SourceName}/` and contains:

| File | Purpose |
|------|---------|
| `bootstrap.py` | The scraper -- implements `fetch_all()`, `fetch_updates()`, `normalize()` |
| `config.yaml` | Source metadata, auth type, rate limits, schema |
| `sample/` | 10+ sample documents for validation |
| `README.md` | Documentation about the data source |
| `.env.template` | Required API keys or credentials (if any) |
| `retrieve.py` | Reference resolver (e.g., "article 1240 code civil" -> document) |

### Two Data Models

**Legislation** (mutable): Laws get amended. Same ID, new content. Strategy: upsert with version tracking.

**Case law** (immutable): Court decisions don't change after publication. Strategy: append-only with dedup.

### Standard Output Schema

Every scraper normalizes documents to a common schema:
- `_id` -- Unique identifier
- `_source` -- Source identifier (e.g., `FR/LegifranceCodes`)
- `_type` -- `legislation` or `case_law`
- `title` -- Document title
- `text` -- Full text content
- `date` -- Publication or decision date
- `url` -- Link to the original source

## Architecture

```
legal-sources/
  manifest.yaml          # Master inventory: all 227 sources + status
  runner.py              # CLI: run, test, and manage scrapers
  common/                # Shared libraries
    base_scraper.py        Base class all scrapers inherit from
    http_client.py         HTTP client with retries + caching
    rate_limiter.py        Token bucket rate limiter
    storage.py             JSONL storage with deduplication
    validators.py          Schema validation
  templates/             # Templates for new scrapers
    scraper_template.py    Boilerplate for bootstrap.py
    config_template.yaml   Boilerplate for config.yaml
    retrieve_template.py   Boilerplate for retrieve.py
  sources/               # One directory per data source
    {CC}/{Source}/          (see per-source structure above)
```

## Coverage

| Region | Countries | Sources |
|--------|-----------|---------|
| EU Member States | AT, BE, BG, CY, CZ, DE, DK, EE, ES, FI, FR, GR, HR, HU, IE, IT, LT, LU, LV, MT, NL, PL, PT, RO, SE, SI, SK | 130+ |
| EFTA / EEA | CH, NO, IS, LI | 10+ |
| Council of Europe | UK, TR, UA, GE, AM, AZ, MD | 20+ |
| Western Balkans | RS, BA, ME, AL, MK, XK | 15+ |
| Other | US, CA, AR, TW, EG | 10+ |

Track live progress on the [dashboard](https://worldwidelaw.github.io/legal-sources/).

## Contributing

We welcome contributions from developers, legal researchers, and **especially governments** who want their legal data included.

**Submit a data source** (no coding required):
- [Open a "New Source" issue](https://github.com/worldwidelaw/legal-sources/issues/new?template=new-source.yml) and tell us about your country's legal data portal

**Fix or improve a scraper**:
- See [CONTRIBUTING.md](CONTRIBUTING.md) for the full guide

**Report a problem**:
- [Data quality issue](https://github.com/worldwidelaw/legal-sources/issues/new?template=data-quality.yml) -- missing or incorrect data
- [Bug report](https://github.com/worldwidelaw/legal-sources/issues/new?template=bug-report.yml) -- broken scraper

## License

[Apache License 2.0](LICENSE) -- use it freely, contribute back if you can.
