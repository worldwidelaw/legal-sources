# HR/AllCourts - Croatian Court Decisions (odluke.sudovi.hr)

## Overview

This source fetches Croatian court decisions from **odluke.sudovi.hr**, the official searchable database of decisions from all Croatian courts.

- **Website**: https://odluke.sudovi.hr
- **Data types**: Case law (court decisions)
- **Authentication**: None required
- **Coverage**: 900,000+ decisions from all Croatian courts
- **Language**: Croatian (HRV)
- **License**: [Open Government Data](https://data.gov.hr/en)

## Courts Covered

The database includes decisions from:
- **Vrhovni sud** (Supreme Court)
- **Visoki kazneni sud** (High Criminal Court)
- **Visoki prekršajni sud** (High Misdemeanor Court)
- **Visoki trgovački sud** (High Commercial Court)
- **Visoki upravni sud** (High Administrative Court)
- **Županijski sudovi** (County Courts)
- **Općinski sudovi** (Municipal Courts)
- **Trgovački sudovi** (Commercial Courts)
- **Upravni sudovi** (Administrative Courts)

**NOTE**: Constitutional Court (Ustavni sud) decisions are NOT included in this database. They are available separately at sljeme.usud.hr (Lotus Notes/Domino system).

## Data Access

Server-side rendered HTML pages, no JavaScript required for content access.

### Endpoints

| Endpoint | Description |
|----------|-------------|
| `/Document/DisplayList?page=N` | Paginated search results |
| `/Document/View?id={uuid}` | Individual decision full text |
| `/Document/DownloadPDF?id={uuid}` | PDF download |

## Usage

```bash
# Test connectivity
python bootstrap.py test

# Fetch sample records (12+)
python bootstrap.py bootstrap --sample

# Fetch all records (full bootstrap)
python bootstrap.py bootstrap

# Incremental update since date
python bootstrap.py update --since 2024-01-01
```

## Output Schema

Each record includes:

| Field | Description |
|-------|-------------|
| `_id` | Document UUID |
| `_source` | "HR/AllCourts" |
| `_type` | "case_law" |
| `title` | Decision identifier and court |
| `text` | **Full text of the decision** |
| `date` | Decision date (ISO 8601) |
| `url` | Link to original document |
| `decision_number` | Case number |
| `court` | Court name |
| `decision_type` | Type of decision (Presuda, Rješenje, etc.) |
| `publication_date` | Date published to database |
| `registry_type` | Court registry type |
| `finality` | Whether decision is final |
| `legal_field` | Legal area (EuroVoc classification) |
| `ecli` | European Case Law Identifier (if available) |

## Sample Statistics

- 12 sample records fetched
- Average 2,763 characters per document
- Courts represented: Supreme Court, High Commercial Court, Municipal Courts, etc.

## License

[Open Government Data](https://data.gov.hr/en) — Croatian court decisions are freely reusable under open government data terms.

## References

- [Croatian Courts Portal](https://sudovi.hr)
- [e-Justice - Croatia](https://e-justice.europa.eu/content_ecli_search-430-hr-en.do)
