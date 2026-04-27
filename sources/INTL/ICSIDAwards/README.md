# INTL/ICSIDAwards — ICSID Arbitration Awards (World Bank)

Investment arbitration awards, decisions, and orders from ICSID.

## Data Source

- **Website**: https://icsid.worldbank.org/cases
- **API**: JSON endpoint for case metadata + HTML scrape for document links
- **Auth**: None required
- **Total**: 1,130 cases (821 concluded), ~1,400 documents

## Strategy

1. Fetch all case metadata from `/api/all/cases` JSON endpoint
2. For concluded cases, scrape case detail pages for PDF document links
3. Download PDFs from `icsidfiles.worldbank.org` (no Cloudflare protection)
4. Extract full text using PyMuPDF

## Usage

```bash
python bootstrap.py test                        # Quick connectivity test
python bootstrap.py bootstrap --sample          # Fetch 15 sample records
python bootstrap.py bootstrap                   # Full fetch (~1,400 documents)
python bootstrap.py update                      # Fetch recent decisions
```

## License

[ICSID Terms](https://icsid.worldbank.org/terms-and-conditions) — published awards are publicly available. Verify World Bank/ICSID terms before commercial redistribution.

## Notes

- ~50% of concluded cases have published documents
- Documents include awards, jurisdiction decisions, provisional measures, annulment decisions
- Older PDFs may be scanned images with limited text extraction
- Rate limited to ~1 request per second
