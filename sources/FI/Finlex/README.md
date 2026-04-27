# FI/Finlex -- Finnish Legal Database

## Overview

[Finlex](https://www.finlex.fi) is Finland's official legal database, operated by the Ministry of Justice. It provides comprehensive access to Finnish legislation, treaties, government proposals, and authority regulations.

## Data Access

**API Endpoint:** `https://opendata.finlex.fi/finlex/avoindata/v1`

**Authentication:** None required (User-Agent header mandatory)

**Format:** Akoma Ntoso XML (international legal document standard)

## Available Data Types

| Type | Endpoint | Description |
|------|----------|-------------|
| `statute` | `/akn/fi/act/statute/list` | Original statutes as published |
| `statute-consolidated` | `/akn/fi/act/statute-consolidated/list` | Up-to-date consolidated law |
| `treaty` | `/akn/fi/doc/treaty/list` | International treaties |
| `government-proposal` | `/akn/fi/doc/government-proposal/list` | Government bills |

**Note:** Case law (Supreme Court/KKO, Supreme Administrative Court/KHO) requires authentication and is not included in this implementation.

## Languages

- Finnish (fin) - primary
- Swedish (swe) - official translation
- Sámi languages (sme, smn, sms) - minority language translations
- English (eng) - unofficial translations of major acts

## License

- Original statutes: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)
- Translations: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)
- Consolidated legislation: [CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/)

> ⚠️ **Consolidated legislation restricts commercial use** (CC BY-NC 4.0). Original statutes and translations are freely reusable.

## Usage

```bash
# Test API connectivity
python bootstrap.py test-api

# Fetch sample records (10 documents)
python bootstrap.py bootstrap --sample

# Full bootstrap (large dataset)
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## API Documentation

- [Integration Quick Guide](https://www.finlex.fi/en/open-data/integration-quick-guide)
- [Open Data Portal](https://www.finlex.fi/en/open-data)
- [Swagger UI](https://opendata.finlex.fi/swagger-ui/index.html)

## Technical Notes

- Pagination: `page` and `limit` parameters (max 100 per page)
- Rate limiting: API may return HTTP 429 on excessive requests
- TLS 1.2+ required (no HTTP)
- Documents are in Akoma Ntoso XML format with full structured text
