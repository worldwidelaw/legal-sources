# PL/KIS-EUREKA — Polish Tax Interpretations (EUREKA System)

**Source**: Polish Ministry of Finance — EUREKA System
**URL**: https://eureka.mf.gov.pl/
**Type**: doctrine (tax interpretations)
**Coverage**: ~541,000 documents
**Auth**: None (public REST API)

## Data Types

- Individual tax interpretations (508,999)
- Binding rate information (22,985)
- Changes to individual interpretations (5,388)
- Binding excise information (1,045)
- General interpretations, tax explanations, official letters (~3,000)

## API

REST API at `https://eureka.mf.gov.pl/api/public/v1`:
- `POST /wyszukiwarka/informacje?size=N&page=P` — Search with filters
- `GET /informacje/{id}` — Full document with HTML text

Full text is in the `TRESC_INTERESARIUSZ` field of each document.

## Usage

```bash
python bootstrap.py test-api              # Test connectivity
python bootstrap.py bootstrap --sample    # Fetch 15 sample records
python bootstrap.py bootstrap             # Full fetch (541k documents)
python bootstrap.py update                # Recent 30 days
```

## License

[Open Government Data](https://dane.gov.pl) — Polish Ministry of Finance public data, free for reuse.
