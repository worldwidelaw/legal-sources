# PH/SCELibrary — Philippines Supreme Court E-Library

## Source
- **URL**: https://elibrary.judiciary.gov.ph
- **Type**: case_law
- **Auth**: None (free public access)
- **Coverage**: 1996–present, ~30,000+ Supreme Court decisions

## Strategy
1. Iterate monthly index pages at `/thebookshelf/docmonth/{Mon}/{Year}/1`
2. Extract decision links (`/thebookshelf/showdocs/1/{doc_id}`)
3. Fetch each decision page and extract full text from `div.single_content`
4. Parse GR number, date, and title from the decision text header

## Usage
```bash
python bootstrap.py test               # Connectivity test
python bootstrap.py bootstrap --sample # Fetch ~15 sample records
python bootstrap.py bootstrap          # Full fetch (all years)
python bootstrap.py update             # Last 3 months only
```

## Notes
- Rate limited to 1 request per 2 seconds
- Full text includes majority opinions, dissents, and footnotes
- GR numbers follow patterns: G.R. No., A.M. No., A.C. No., UDK-XXXXX

## License

[Open Government Data](https://elibrary.judiciary.gov.ph) — official decisions published by the Supreme Court of the Philippines.
