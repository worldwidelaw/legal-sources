# SE/SvenskaForfattningssamlingen - Swedish Legislation (SFS)

## Overview

This data source fetches Swedish legislation from the Riksdag (Swedish Parliament) open data API.

**Source:** Sveriges riksdag (Swedish Parliament)  
**URL:** https://data.riksdagen.se  
**Document Type:** Legislation  
**Language:** Swedish (sv)  
**License:** [Public Domain](https://data.riksdagen.se) — Swedish official documents are not subject to copyright (Upphovsrattslag 1960:729, Section 9).

## Data Coverage

- **Svenska Författningssamlingen (SFS)** - The official Swedish Code of Statutes
- Over 11,000 documents available
- Includes laws (lagar), ordinances (förordningar), and regulations
- Date range: Historical legislation through present day

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `/dokumentlista/?doktyp=sfs&utformat=json` | List all SFS documents with pagination |
| `/dokument/{dok_id}.text` | Full text of a document |
| `/dokument/{dok_id}.json` | Full JSON metadata of a document |
| `/dokument/{dok_id}.html` | HTML formatted document |

### Query Parameters

- `doktyp=sfs` - Filter to SFS documents only
- `from=YYYY-MM-DD` - Documents from this date onwards
- `tom=YYYY-MM-DD` - Documents up to this date
- `p=N` - Page number
- `sort=datum` - Sort by date
- `sortorder=desc` - Descending order

## Usage

```bash
# Test with 3 documents
python3 bootstrap.py

# Bootstrap with 10 sample documents
python3 bootstrap.py bootstrap --sample

# Bootstrap with 100 documents
python3 bootstrap.py bootstrap
```

## Output Schema

```json
{
  "_id": "sfs-2026-62",
  "_source": "SE/SvenskaForfattningssamlingen",
  "_type": "legislation",
  "_fetched_at": "2026-02-10T...",
  "title": "Law title in Swedish",
  "subtitle": "Subtitle if any",
  "sfs_number": "2026:62",
  "text": "Full text of the law...",
  "date": "2026-02-05",
  "published": "2026-02-06",
  "url": "https://www.riksdagen.se/sv/...",
  "language": "sv",
  "summary": "First paragraph summary",
  "organ": "Issuing authority",
  "document_name": "Svensk författningssamling"
}
```

## License

[Public Domain](https://data.riksdagen.se) — Swedish official documents are not subject to copyright (Upphovsrattslag 1960:729, Section 9).

## Notes

- The API is rate-limited; fetcher uses 1.5 second delays between requests
- Full text is fetched separately from the list endpoint
- No authentication required
- All data is public domain
