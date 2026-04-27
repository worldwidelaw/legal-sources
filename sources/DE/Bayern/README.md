# DE/Bayern - Bavaria State Law (BAYERN.RECHT)

Fetches court decisions from all Bavarian courts via the RSS API.

## Data Coverage

- **23,000+ court decisions** across all Bavarian courts
- **2,491 legislation documents** (not yet implemented)
- Courts covered:
  - Verfassungsgerichtshof Bayern (Constitutional Court)
  - Bayerischer Verwaltungsgerichtshof (VGH)
  - Oberlandesgerichte (OLG München, Nürnberg, Bamberg)
  - Landgerichte (LG)
  - Amtsgerichte (AG)
  - Verwaltungsgerichte (VG)
  - Arbeitsgerichte
  - Sozialgerichte
  - Finanzgerichte

## Data Source

- **Website:** https://www.gesetze-bayern.de
- **RSS Feed:** https://www.gesetze-bayern.de/Api/Feed
- **Document URL pattern:** `/Content/Document/{guid}`

## Usage

```bash
# Fetch sample records for validation
python3 bootstrap.py bootstrap --sample --count 15

# Check RSS feed status
python3 bootstrap.py status

# Fetch recent updates
python3 bootstrap.py update
```

## Schema

Each record contains:

| Field | Type | Description |
|-------|------|-------------|
| `_id` | string | Unique ID (BY-{guid}) |
| `_source` | string | "DE/Bayern" |
| `_type` | string | "case_law" |
| `title` | string | Decision title/subject |
| `text` | string | Full text (tenor + reasons) |
| `date` | string | Decision date (YYYY-MM-DD) |
| `url` | string | Link to original document |
| `court_name` | string | Court name (e.g., "VGH München") |
| `file_number` | string | Case file number |
| `keywords` | string | Subject matter keywords |
| `norm_chain` | string | Referenced legal norms |
| `citation` | string | BeckRS citation |

## License

Public domain under German law — [§ 5 UrhG](https://www.gesetze-im-internet.de/urhg/__5.html) (official works / amtliche Werke).

## Notes

- RSS feed provides recent decisions only
- Full historical coverage would require search pagination (not yet implemented)
- Full text is extracted from HTML structure (Tenor + Gründe sections)
