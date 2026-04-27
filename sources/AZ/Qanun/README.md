# AZ/Qanun - Azerbaijan Legislation Database (e-Qanun)

Official legal database maintained by the Ministry of Justice of the Republic of Azerbaijan.

## Data Source

- **URL**: https://e-qanun.az
- **API**: https://api.e-qanun.az
- **Coverage**: 1992-present
- **Language**: Azerbaijani

## Document Types

- Laws (Qanunlar)
- Presidential Decrees (Fərmanlar)
- Cabinet Decisions (Qərarlar)
- Ministerial Orders
- Other legal acts

## Data Access

1. **Sitemap**: https://e-qanun.az/sitemap.xml - lists all framework document IDs
2. **Metadata API**: https://api.e-qanun.az/framework/{id} - returns JSON metadata
3. **Full Text**: URL from API's `htmlUrl` field - Microsoft Word exported HTML

## Usage

```bash
# Quick connectivity test
python bootstrap.py test

# Fetch 10+ sample records for validation
python bootstrap.py bootstrap --sample

# Full bootstrap (55,000+ documents)
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## Notes

- Full text HTML requires Referer header set to https://e-qanun.az/
- HTML files are Microsoft Word exports, text extraction handles Word-specific markup
- Document IDs are sequential; higher IDs are more recent

## License

[Public Government Data](https://e-qanun.az) — official legislation published by the Ministry of Justice of the Republic of Azerbaijan.
