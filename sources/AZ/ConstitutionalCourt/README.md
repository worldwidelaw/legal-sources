# Azerbaijan Constitutional Court

**Source ID:** AZ/ConstitutionalCourt
**Data Type:** Case Law
**Country:** Azerbaijan
**Language:** Azerbaijani

## Description

This scraper collects decisions from the Constitutional Court of the Republic of Azerbaijan (Azərbaycan Respublikası Konstitusiya Məhkəməsi).

## Data Source

- **URL:** https://www.constcourt.gov.az
- **Access:** Public, no authentication required
- **Format:** HTML (with Word download option)

## Coverage

- Constitutional Court decisions from 1998 onwards
- Estimated 600+ decisions
- Types of decisions:
  - Qərar (Decision)
  - Qərardad (Ruling/Decree)
  - Rəy (Opinion)
  - Şərh (Interpretation)

## Technical Details

### Endpoints Used

1. **Decision List:** `/az/decisions?page={N}` - Paginated list of decisions (~15 per page)
2. **Decision Detail:** `/az/decision/{id}` - Full text of individual decision
3. **Word Download:** `/az/decisionDocx/{id}` - Word document version (optional)

### Rate Limiting

- 2 seconds between requests
- No authentication required

### SSL Note

The site has SSL certificate issues. The scraper is configured to accept unverified certificates.

## Usage

```bash
# Fetch sample records
python bootstrap.py bootstrap --sample

# Full bootstrap
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## Data Schema

Each normalized record contains:

| Field | Description |
|-------|-------------|
| `_id` | Unique identifier (AZ/ConstitutionalCourt/{decision_id}) |
| `title` | Decision title (in Azerbaijani) |
| `text` | Full text of the decision |
| `date` | Decision date (ISO format) |
| `url` | Link to original source |
| `decision_id` | Internal decision ID |
| `decision_type` | Type classification |
| `docx_url` | Link to Word document (if available) |

## License

Public government data. Original content is published by the Azerbaijan Constitutional Court.
