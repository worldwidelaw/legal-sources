# INTL/ICJDecisions — International Court of Justice Decisions

Judgments, advisory opinions, and orders from the International Court of Justice (ICJ) since 1946.

## Data Source

- **Website**: https://icj-cij.org/decisions
- **Format**: HTML listing page + PDF documents
- **Auth**: None required
- **Total**: ~872 decisions (~157 judgments, ~31 advisory opinions, ~683 orders)

## Strategy

1. Parse the ICJ decisions list page for metadata and PDF URLs
2. Download PDFs from the UN cloud CDN (`icj-web.leman.un-icc.cloud`) which serves the same files without Cloudflare JS challenges
3. Extract full text from PDFs using PyMuPDF

## Usage

```bash
python bootstrap.py test                        # Quick connectivity test
python bootstrap.py bootstrap --sample          # Fetch 15 sample records
python bootstrap.py bootstrap                   # Full fetch (~870 PDFs)
python bootstrap.py update                      # Fetch recent decisions
```

## Fields

| Field | Description |
|-------|-------------|
| `title` | Decision title with case name |
| `text` | Full text extracted from PDF |
| `date` | Decision date (ISO 8601) |
| `case_name` | Case name (parties) |
| `case_id` | ICJ case number |
| `decision_type` | judgment, advisory_opinion, or order |
| `court` | International Court of Justice |

## Notes

- PDFs on `icj-cij.org` are behind Cloudflare JS challenges; the UN cloud CDN serves the same files without protection
- ICJ documents are bilingual (English/French); PDFs contain both languages
- Rate limited to ~1 request per 1.5 seconds
