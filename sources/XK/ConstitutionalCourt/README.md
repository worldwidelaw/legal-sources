# XK/ConstitutionalCourt — Kosovo Constitutional Court

## Source Overview

- **Name**: Kosovo Constitutional Court (Gjykata Kushtetuese)
- **Country**: Kosovo (XK)
- **Data Type**: Case Law
- **Total Records**: ~3,000 decisions
- **Languages**: Albanian (sq), Serbian (sr)
- **URL**: https://gjk-ks.org

## Data Access

This source uses an official REST API at `https://api.webgjk-ks.org`.

### API Endpoints

- **POST** `/publish/CdmsCase/getAllFilteredDecisions` - Paginated list of decisions
- **GET** `/publish/CdmsCase/getFilterActTree` - Filter categories (decision types)

### Request Format

```json
{
  "PageNumber": 1,
  "PageSize": 100
}
```

### Response Format

```json
{
  "status": 0,
  "data": [
    {
      "id": 9298,
      "caseNumber": "KI134/24",
      "title": "...",
      "titleSR": "...",
      "description": "<html>..full text in Albanian..</html>",
      "descriptionSR": "<html>..full text in Serbian..</html>",
      "entryDate": "2026-02-25T00:00:00",
      "complainant": "...",
      "documents": [
        {"documentUrl": "https://api.webgjk-ks.org/Custom/{uuid}.pdf", "idLanguage": 1}
      ],
      "totalCount": 2954
    }
  ]
}
```

## Coverage

The Kosovo Constitutional Court was established in 2009 following Kosovo's declaration of independence in 2008. This source includes:

- Judgments (Aktgjykim / Presuda)
- Resolutions (Aktvendim / Rešenje)
- Decisions (Vendim / Odluka)
- Dissenting and concurring opinions

All decisions include full text in Albanian and Serbian.

## Usage

```bash
# Test API connectivity
python bootstrap.py test-api

# Fetch sample (10-20 records)
python bootstrap.py bootstrap --sample

# Full historical fetch
python bootstrap.py bootstrap

# Incremental update (last 30 days)
python bootstrap.py update
```

## License

Open government data — publicly accessible court decisions.

## Schema

| Field | Description |
|-------|-------------|
| `_id` | Case number (e.g., "KI134/24") |
| `text` | Full text in Albanian + Serbian |
| `title` | Case title in Albanian |
| `title_sr` | Case title in Serbian |
| `date` | Publication date (ISO 8601) |
| `case_number` | Official case number |
| `decision_type` | Judgment, Resolution, etc. |
| `complainant` | Name of complainant |
| `pdf_urls` | Links to PDF documents |
| `languages` | ["sq", "sr"] |
