# IS/Yfirskattanefnd — Icelandic Tax Appeals Board Rulings

Fetches rulings from the Yfirskattanefnd (Internal Revenue Board / Tax Appeals Board),
Iceland's supreme administrative appeals authority for taxation, VAT, and duties.

- **Source**: https://yskn.is/urskurdir/
- **Coverage**: 1973–present (~7,000+ decisions)
- **Language**: Icelandic
- **Type**: case_law (administrative tax appeal decisions)
- **Full text**: Yes — complete rulings with facts, legal reasoning, and conclusions

## How it works

1. Iterates year-by-year listing pages (`?year=YYYY`) to discover ruling IDs
2. Fetches each ruling via AJAX endpoint (`?nr=ID&altTemplate=SkodaurskurdAjax`)
3. Parses HTML to extract ruling number, date, keywords, and full text

## License

[Public Domain](https://www.government.is/publications/legislation/) — Official government administrative decisions are public domain under Icelandic law.
