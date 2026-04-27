# BE/FISCONETplus-Doctrine

Belgian tax doctrine from FISCONETplus (FPS Finance / SPF Finances).

## Data Source

- **Portal**: https://eservices.minfin.fgov.be/myminfin-web/pages/fisconet
- **API**: `https://www.minfin.fgov.be/myminfin-rest/fisconetPlus/public/`
- **Auth**: None (public API)
- **Format**: JSON API with base64-encoded HTML full text

## Document Types

| Type | Count | Description |
|------|-------|-------------|
| Circular letters | ~3,700 | Tax administration circulars |
| Prior agreements | ~15,600 | Advance tax rulings (L 24.12.2002) |
| Comments | ~6,300 | Administrative commentaries |
| Communications | ~1,300 | Official communications |
| Decisions | ~1,500 | Administrative decisions |
| **Total** | **~28,400** | |

## Usage

```bash
python bootstrap.py test                # Connectivity test
python bootstrap.py bootstrap --sample  # Fetch 15 sample records
python bootstrap.py bootstrap           # Full fetch (~28k docs)
python bootstrap.py update --since 2026-01-01  # Incremental
```

## Languages

Documents are available in French, Dutch, German, and English.

## License

[Belgian Open Government Data](https://data.gov.be/en/licence-conditions) — free reuse of Belgian public sector information.
