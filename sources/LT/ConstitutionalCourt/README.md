# LT/ConstitutionalCourt - Lithuanian Constitutional Court

Data fetcher for Constitutional Court of the Republic of Lithuania (Lietuvos Respublikos Konstitucinis Teismas) decisions.

## Data Source

- **Official website**: https://www.lrkt.lt
- **Data API**: https://get.data.gov.lt (TAR - Register of Legal Acts)
- **Dataset**: https://data.gov.lt/datasets/2613/

## Coverage

- **Document types**: Rulings (nutarimai), Decisions (sprendimai), Conclusions (išvados), Announcements (pranešimai)
- **Time period**: 1993 onwards (since CC establishment)
- **Total documents**: ~720 (as of 2026-02)
- **Full text**: Yes (in Lithuanian)
- **License**: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)

## Data Access

The Constitutional Court's website (lrkt.lt) is protected by Cloudflare, but all decisions are published in the official Register of Legal Acts (TAR) which is accessible via the data.gov.lt Open Data API.

Documents are filtered by `priemusi_inst.contains("Konstitucinis")` to select only Constitutional Court publications.

### API Endpoints

```
GET https://get.data.gov.lt/datasets/gov/lrsk/teises_aktai/Dokumentas?priemusi_inst.contains("Konstitucinis")&_limit=100
```

### Pagination

Uses vda_id-based keyset pagination (cursor-based pagination has issues when combined with filters):

```
GET ...&vda_id>"{last_vda_id}"&_sort=vda_id&_limit=100
```

## Usage

```bash
# Test connectivity
python bootstrap.py test

# Fetch sample records (12 by default)
python bootstrap.py bootstrap --sample

# Full bootstrap
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## Sample Data

Sample records are stored in `sample/` directory with:
- Individual JSON files per record
- Combined `all_samples.json` with all sample records

## Schema

Key fields from normalized records:

| Field | Description |
|-------|-------------|
| `_id` | Document ID (dokumento_id) |
| `title` | Document title |
| `text` | Full text content (MANDATORY) |
| `date` | Decision date (YYYY-MM-DD) |
| `url` | Link to e-tar.lt |
| `document_type` | Type in English (ruling/decision/conclusion) |
| `document_type_lt` | Type in Lithuanian |
| `court` | Court name in Lithuanian |
| `court_en` | Court name in English |

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — Lithuanian open government data.

## Notes

- All text is in Lithuanian
- Document types include:
  - Nutarimas (Ruling) - constitutional review decisions
  - Sprendimas (Decision) - procedural decisions
  - Išvada (Conclusion) - advisory opinions
  - Pranešimas (Announcement) - administrative notices
- Average document length: ~62,000 characters
