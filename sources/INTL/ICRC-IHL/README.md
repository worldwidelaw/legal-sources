# INTL/ICRC-IHL — ICRC International Humanitarian Law Databases

Three IHL sub-databases from the ICRC: treaties, customary rules, and national practice.

## Data Source

- **Website**: https://ihl-databases.icrc.org
- **API**: Drupal JSON:API (no auth required)
- **Total**: ~6,634 documents (111 treaties + 169 rules + 6,354 national practice)

## Sub-databases

| Database | Items | Type | Content |
|----------|-------|------|---------|
| Treaties | 111 | legislation | Full article text + ICRC presentation |
| Customary IHL Rules | 169 | doctrine | Rule text with practice analysis |
| National Practice | 6,354 | legislation/case_law/doctrine | Summaries of national implementation |

## Usage

```bash
python bootstrap.py test                        # Quick connectivity test
python bootstrap.py bootstrap --sample          # Fetch 15 sample records (5 per sub-db)
python bootstrap.py bootstrap                   # Full fetch (~6,634 documents)
python bootstrap.py update                      # Fetch recent changes
```

## License

[ICRC Terms of Use](https://www.icrc.org/en/terms-and-conditions) — ICRC databases are provided for informational/research purposes. Verify terms before commercial redistribution.

## Notes

- JSON:API endpoints require `--globoff` flag with curl (URLs contain `[]`)
- Treaty full text fetched via `include=field_treaty_content` relationship
- National practice categories: Legislation (4,860), Case-law (880), Manual (586), Other (28)
- Content available in 7 languages; we fetch English
