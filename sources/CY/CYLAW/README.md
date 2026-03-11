# CY/CYLAW - Cyprus Legislation Database

## Overview
- **Source ID:** CY/CYLAW
- **Country:** Cyprus (CY)
- **Data Type:** Legislation
- **Language:** Greek (el)
- **Coverage:** Consolidated Cypriot legislation

## Data Source
The scraper fetches legislation from **CyLaw** (www.cylaw.org), operated by the Cyprus Bar Association.

### Data Endpoint
- **XML Export:** `http://www.cylaw.org/nomoi/enop/backup/cybarlegis/zips/export/full20240628-0951/db/cybar/legislation/`
- **Contents Index:** `__contents__.xml` provides a list of all documents
- **Individual Documents:** `{doc_id}.xml` for each law

### Document Format
Documents are in eXist-db XML format with the following structure:
- `<legislation>` root element with metadata attributes
- `<form>` section with law number, official title, short title
- `<legis-body>` containing the full text organized by divisions and sections
- `<appendix>` for schedules and annexes

## Document Types
- **Ενοποιημένη Νομοθεσία (Consolidated Legislation):** Laws with all amendments integrated
- Various types including laws, decrees, regulations

## Technical Notes
- **Encoding:** UTF-8
- **Format:** Structured XML
- **Rate Limiting:** 1 request/second
- **Document Count:** 500+ consolidated laws

## Usage
```bash
# Fetch sample records
python3 bootstrap.py bootstrap --sample

# Fetch all records (with limit)
python3 bootstrap.py bootstrap --max 100

# Fetch updates since a date
python3 bootstrap.py updates --since 2024-01-01
```

## License
Open Government Data - Cyprus legislation is publicly available through CyLaw.

## References
- [CyLaw Official Website](https://www.cylaw.org)
- [N-Lex Cyprus](https://n-lex.europa.eu/n-lex/legis_cy/cy_gov_cyprus_form)
- [Cyprus Bar Association](https://www.cyprusbar.org)
