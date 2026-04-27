# PL/SupremeCourt - Polish Supreme Court (Sąd Najwyższy)

Polish Supreme Court case law via the SAOS (System Analizy Orzeczeń Sądowych) API.

## Data Source

- **API**: https://www.saos.org.pl/api
- **Documentation**: https://www.saos.org.pl/help/index.php/dokumentacja-api
- **Web Interface**: https://www.saos.org.pl/search
- **Coverage**: 38,000+ Supreme Court judgments with full text

## Court Structure

The Polish Supreme Court (Sąd Najwyższy) has four chambers:

1. **Izba Karna** - Criminal Chamber
2. **Izba Cywilna** - Civil Chamber
3. **Izba Pracy, Ubezpieczeń Społecznych i Spraw Publicznych** - Labor, Social Insurance, and Public Affairs Chamber
4. **Izba Wojskowa** - Military Chamber

## API Endpoints Used

| Endpoint | Purpose |
|----------|---------|
| `/api/search/judgments?courtType=SUPREME` | Search and paginate judgments |
| `/api/judgments/{id}` | Get full judgment details |
| `/api/dump/scChambers` | List Supreme Court chambers |

## Data Fields

| Field | Description |
|-------|-------------|
| `case_number` | Case docket number (e.g., "III CSK 123/20") |
| `judgment_date` | Date of judgment |
| `text` | Full text of the judgment (required) |
| `chamber` | Court chamber (Izba) |
| `judges` | List of judges with roles |
| `judgment_type` | SENTENCE, RESOLUTION, DECISION, etc. |
| `legal_bases` | Legal foundations cited |
| `referenced_regulations` | Laws and regulations cited |

## Usage

```bash
# Test API connectivity
python bootstrap.py test-api

# Fetch sample records (12 judgments)
python bootstrap.py bootstrap --sample

# Full bootstrap (38K+ records - use with caution)
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## License

[Public Domain](https://dane.gov.pl) — Polish court decisions are public domain. SAOS is operated by ICM (University of Warsaw) in partnership with the Ministry of Justice.

## Notes

- The search API returns truncated `textContent`; the detail API returns full text
- Pagination uses `pageNumber` (0-indexed) and `pageSize` (1-100)
- Rate limiting: 2 requests/second recommended
- Full text averages 5,000-15,000 characters per judgment
