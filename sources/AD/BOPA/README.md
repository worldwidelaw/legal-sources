# AD/BOPA -- Andorra Official Gazette

**Butlletí Oficial del Principat d'Andorra (BOPA)**

## Overview

This source fetches Andorran legislation from the official gazette at bopa.ad.

- **Country:** Andorra (AD)
- **Language:** Catalan (ca)
- **Data type:** Legislation
- **Coverage:** 2015-present (electronic official version)
- **License:** Open Government Data

## Data Access

The BOPA website is a React SPA backed by Azure Functions API:

- **API Base:** `https://bopaazurefunctions.azurewebsites.net/api/`
- **Storage:** `https://bopadocuments.blob.core.windows.net/bopa-documents/`

### API Endpoints

1. **GetNewPaginatedNewsletter** (POST) - List BOPA issues with pagination
2. **GetDocumentsByBOPA** (GET) - List documents in a specific BOPA issue
3. **GetFilters** (GET) - Get filter options (organismes, temes)

### Document Types

- **Lleis** - Laws from Consell General (Parliament)
- **Convenis internacionals** - International agreements
- **Reglaments** - Regulations from Govern (Government)
- **Decrets** - Decrees
- **Altres disposicions** - Other dispositions

## Usage

```bash
# Quick connectivity test
python bootstrap.py test

# Fetch sample records for validation
python bootstrap.py bootstrap --sample

# Full bootstrap (all documents)
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## Document Structure

Each normalized document contains:

| Field | Description |
|-------|-------------|
| `_id` | Unique identifier (AD_BOPA_YEAR_NUM_NAME) |
| `_source` | Source identifier (AD/BOPA) |
| `_type` | Document type (legislation) |
| `title` | Document summary (sumari) |
| `text` | Full text extracted from HTML |
| `date` | Publication date |
| `url` | Link to document on bopa.ad |
| `document_name` | Internal document name |
| `bopa_year` | BOPA year |
| `bopa_number` | BOPA issue number |
| `organisme_pare` | Parent organization |
| `organisme` | Specific organization |
| `tema` | Topic/theme |
| `document_type` | law/regulation/decree/etc |
| `language` | ca (Catalan) |

## Notes

- Electronic BOPA became the official version on January 1, 2015
- Prior to 2015, only paper version was legally official
- API codes are embedded in the bopa.ad React application JavaScript
- Full text is stored as HTML in Azure Blob Storage
- Documents are filtered to exclude job postings, contract awards, etc.

## Legal

Per Law 25/2014 (October 30) on BOPA:
- The electronic version at www.bopa.ad is authentic and legally valid from 2015
- Content is open government data
