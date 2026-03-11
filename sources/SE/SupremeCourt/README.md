# SE/SupremeCourt - Swedish Supreme Court Case Law

Fetches Swedish court decisions from Domstolsverket's (Swedish Courts Administration) open data REST API.

## Data Source

- **API**: https://rattspraxis.etjanst.domstol.se/api/v1
- **Docs**: https://rattspraxis.etjanst.domstol.se/openapi/puh-openapi.yaml
- **Web UI**: https://rattspraxis.etjanst.domstol.se/sok/
- **Open Data**: https://www.domstol.se/om-webbplatsen-och-digitala-kanaler/oppna-data/

## Coverage

### Courts
- **HDO** - Högsta domstolen (Supreme Court for civil/criminal)
- **HFD** - Högsta förvaltningsdomstolen (Supreme Administrative Court)
- Courts of Appeal (Hovrätter)
- Administrative Courts of Appeal (Kammarrätter)
- Specialized courts (Labour, Migration, Environment, Patent/Market)

### Time Period
- Case summaries: 1981-present
- Full decisions with PDF: March 2025-present

### Publication Types
- `DOM_ELLER_BESLUT` - Judgments and decisions
- `RATTSFALL` - Case reports (NJA/RÅ references)
- `PROVNINGSTILLSTAND` - Leave to appeal decisions
- `FORHANDSAVGORANDE` - Preliminary ruling requests

## Usage

```bash
# List available courts
python3 bootstrap.py courts

# Test API connection
python3 bootstrap.py test

# Fetch sample records (default: Supreme Court HDO)
python3 bootstrap.py bootstrap --count 12

# Fetch from specific court
python3 bootstrap.py bootstrap --court HFD --count 12

# Fetch from all courts
python3 bootstrap.py bootstrap --court '' --count 20

# Stream all records
python3 bootstrap.py fetch --court HDO
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/domstolar` | GET | List courts |
| `/api/v1/publiceringar` | GET | List publications (paginated) |
| `/api/v1/publiceringar/{id}` | GET | Get single publication |
| `/api/v1/bilagor/{storageId}` | GET | Download PDF attachment |
| `/api/v1/sok` | POST | Search publications |

## Full Text Extraction

Full text is extracted from PDF attachments:

1. API returns publication with `bilagaLista` containing PDF references
2. PDF downloaded via `/api/v1/bilagor/{storageId}` (URL-encoded)
3. Text extracted using `pdfplumber`
4. Cleaned to remove page headers/footers

## Output Schema

```json
{
  "_id": "d088b9fe-2b38-4447-9f38-ec0afd4f1b38",
  "_source": "SE/SupremeCourt",
  "_type": "case_law",
  "_fetched_at": "2026-02-14T09:00:00Z",
  "title": "Dödsbodelägares preskriptionsavbrott (T 5816-24)",
  "text": "[Full decision text, 6000+ chars...]",
  "date": "2026-02-13",
  "url": "https://rattspraxis.etjanst.domstol.se/sok/?id=...",
  "court": "Högsta domstolen",
  "court_code": "HDO",
  "primary_case_number": "T 5816-24",
  "is_precedent": true,
  "keywords": ["Preskriptionsavbrott", "Dödsbodelägare", ...],
  "legal_provisions": ["5 § preskriptionslagen (1981:130)"],
  "summary": "...",
  "language": "swe"
}
```

## License

Open data - Swedish court decisions are public domain.

## Notes

- Rate limiting: 1 second delay between requests
- PDF extraction requires `pdfplumber` library
- Some older summaries may not have PDF attachments (text from summary field)
