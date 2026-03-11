# IT/Camera - Italian Chamber of Deputies (Camera dei Deputati)

## Overview

This source fetches parliamentary bills (proposte di legge / progetti di legge) from the
Italian Chamber of Deputies with full text.

- **URL**: https://www.camera.it
- **Data Type**: Legislation (bills)
- **Format**: HTML (with structured content)
- **Authentication**: None required
- **License**: Public information (Camera dei deputati © Tutti i diritti riservati)

## Data Coverage

- **Current Legislature**: XIX (2022-present)
- **Bill Types**: Proposte di legge (legislative proposals), Disegni di legge (government bills)
- **Document Types**: Bill text (relazione + articles), metadata

## API Endpoints

### Bill Info Page
```
https://www.camera.it/leg{legislature}/126?tab=1&leg={legislature}&idDocumento={bill_id}
```

### Full Text HTML
```
https://documenti.camera.it/apps/commonServices/getDocumento.ashx?sezione=lavori&tipoDoc=testo_pdl&idlegislatura={legislature}&codice={doc_code}
```

### Full Text PDF
```
https://documenti.camera.it/apps/commonServices/getDocumento.ashx?sezione=lavori&tipoDoc=testo_pdl_pdf&idlegislatura={legislature}&codice={doc_code}
```

## Data Schema

Each record contains:

| Field | Description |
|-------|-------------|
| `_id` | Unique identifier (e.g., `IT/Camera/leg19/AC1`) |
| `_source` | Source identifier (`IT/Camera`) |
| `_type` | Document type (`legislation`) |
| `_fetched_at` | ISO 8601 timestamp |
| `title` | Bill title |
| `text` | Full text of the bill (relazione + articles) |
| `date` | Presentation date (ISO 8601) |
| `url` | Link to bill page |
| `bill_number` | Chamber act number (e.g., `AC1`) |
| `legislature` | Legislature number |
| `bill_type` | Type of bill (PROPOSTA DI LEGGE, DISEGNO DI LEGGE) |
| `initiative_type` | Initiative origin (popular, government, deputies) |
| `assigned_to` | Committee assignment |

## Usage

### Bootstrap (fetch sample records)
```bash
python3 bootstrap.py bootstrap --sample --count 12
```

### Fetch all bills
```bash
python3 bootstrap.py fetch --legislature 19 --start 1 --max 1000
```

## Notes

- Bill IDs are sequential within each legislature but may have gaps
- Full text includes the explanatory memorandum (relazione) and bill articles
- Some older bills may not have digitized text available
- Rate limiting: 2 second delay between requests
