# US/AK-Courts — Alaska Supreme Court & Court of Appeals

Case law from Alaska's two appellate courts via CourtListener's public search API.

## Courts Covered

| Court | CourtListener ID | Approx. Count |
|-------|-----------------|---------------|
| Supreme Court of Alaska | `alaska` | ~7,000 |
| Court of Appeals of Alaska | `alaskactapp` | ~3,700 |

## Data Access

- **API**: CourtListener REST v4 search endpoint (no auth required)
- **Full text**: PDFs from `storage.courtlistener.com`, extracted via pdfplumber
- **Fallback**: HTML opinions when available

## Usage

```bash
python bootstrap.py test                    # Connectivity check
python bootstrap.py bootstrap --sample      # 15 sample records
python bootstrap.py bootstrap               # Full bootstrap
python bootstrap.py update --since 2025-01-01  # Incremental
```

## License

[Public domain](https://www.law.cornell.edu/uscode/text/17/105) — US government works under 17 U.S.C. § 105.
