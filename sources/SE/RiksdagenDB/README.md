# SE/RiksdagenDB - Swedish Parliament Documents

Data source for Swedish Parliamentary documents from the Riksdag (Swedish Parliament).

## Data Source

- **API**: https://data.riksdagen.se
- **Portal**: https://www.riksdagen.se/sv/dokument-och-lagar/
- **License**: [Public Domain](https://data.riksdagen.se) — Swedish official documents are not subject to copyright (Upphovsrattslag 1960:729, Section 9).

## Document Types

| Code | Swedish | English | Count |
|------|---------|---------|-------|
| prop | Proposition | Government Bill | 31K+ |
| mot | Motion | Member Motion | 257K+ |
| bet | Betänkande | Committee Report | 74K+ |
| rskr | Riksdagsskrivelse | Parliamentary Decision | - |
| prot | Protokoll | Plenary Protocol | - |

## API Endpoints

### Document List
```
GET https://data.riksdagen.se/dokumentlista/?doktyp={type}&utformat=json&p={page}
```

Parameters:
- `doktyp`: Document type (prop, mot, bet, etc.)
- `utformat`: Output format (json, xml)
- `p`: Page number
- `from`: Start date (YYYY-MM-DD)
- `sort`: Sort field (datum)
- `sortorder`: asc or desc

### Individual Document
```
GET https://data.riksdagen.se/dokument/{dok_id}.json
```

Returns full metadata and HTML content in the `dokumentstatus.dokument.html` field.

## Usage

```bash
# Test fetch
python3 bootstrap.py

# Bootstrap sample data (12 documents)
python3 bootstrap.py bootstrap --sample

# Full bootstrap (100 documents)
python3 bootstrap.py bootstrap
```

## Schema

Key fields in normalized output:
- `_id`: Document ID (e.g., "HD03124")
- `_source`: "SE/RiksdagenDB"
- `_type`: "legislation"
- `title`: Document title
- `text`: Full text extracted from HTML
- `date`: Publication date
- `document_type`: Type code (prop, mot, bet, etc.)
- `session`: Parliamentary session (e.g., "2025/26")
- `document_number`: Number within session

## License

[Public Domain](https://data.riksdagen.se) — Swedish official documents are not subject to copyright (Upphovsrattslag 1960:729, Section 9).

## Notes

- This source covers **parliamentary process documents**, not enacted law
- For **enacted legislation** (SFS), use SE/SvenskaForfattningssamlingen
- Full text is extracted from HTML field in JSON response
- Rate limiting: 2 second delay between requests
