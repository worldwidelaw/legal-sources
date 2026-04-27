# IE/SupremeCourt — Irish Courts Service Case Law

Fetches Irish court judgments from the Courts Service of Ireland.

## Data Source

- **URL**: https://www2.courts.ie/Judgments
- **Type**: Case Law
- **Auth**: None (public access)
- **Coverage**: Supreme Court, Court of Appeal, High Court, Circuit Court

## Strategy

1. **Listing Discovery**: Paginate through the Drupal listing at `/Judgments`
2. **PDF URLs**: Extract PDF paths from `href="/acc/alfresco/{uuid}/{citation}.pdf/pdf"`
3. **Full Text**: Download PDFs and extract text using pdfplumber

## Data Format

Each judgment has:
- **Neutral Citation**: e.g., `[2026] IEHC 83` or `[2026] IECA 13`
- **Title**: Case name (parties)
- **Court**: Supreme Court (IESC), Court of Appeal (IECA), High Court (IEHC)
- **Judge**: Presiding judge
- **Date Delivered**: Date judgment delivered
- **Full Text**: Extracted from PDF

## Court Codes

- `IESC` - Supreme Court of Ireland
- `IECA` - Court of Appeal
- `IEHC` - High Court
- `IECC` - Circuit Court

## Usage

```bash
# Test connectivity
python bootstrap.py test

# Fetch sample records
python bootstrap.py bootstrap --sample

# Full bootstrap
python bootstrap.py bootstrap
```

## License

[Irish Public Service Data Licence](https://data.gov.ie/pages/licence) — open access for reuse.
