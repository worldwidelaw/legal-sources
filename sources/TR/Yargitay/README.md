# TR/Yargitay — Turkish Court of Cassation

**Source ID:** `TR/Yargitay`
**Country:** Turkey (TR)
**Type:** Case Law
**Coverage:** ~6 million decisions

## Overview

The Court of Cassation (Yargıtay) is Turkey's supreme court for civil and criminal matters. It reviews decisions from lower courts to ensure uniform application of law across the country.

## Data Source

This scraper uses the **Bedesten API** at `bedesten.adalet.gov.tr`, which provides:

- **Search endpoint:** `/emsal-karar/searchDocuments` - Search by keyword, date range, chamber
- **Document endpoint:** `/emsal-karar/getDocumentContent` - Retrieve full text (base64 HTML)

The API is publicly accessible without authentication.

## Court Structure

### Civil Chambers (Hukuk Daireleri)
- 1-23. Hukuk Dairesi (Civil Divisions 1-23)
- Hukuk Genel Kurulu (Civil General Council)

### Criminal Chambers (Ceza Daireleri)
- 1-23. Ceza Dairesi (Criminal Divisions 1-23)
- Ceza Genel Kurulu (Criminal General Council)

### Special Bodies
- Büyük Genel Kurul (Grand General Council)
- İçtihadı Birleştirme Kurulları (Jurisprudence Unification Councils)

## Usage

```bash
# Sample mode (12 records)
python bootstrap.py bootstrap --sample

# Full bootstrap
python bootstrap.py bootstrap

# Incremental updates
python bootstrap.py update
```

## Data Fields

| Field | Description |
|-------|-------------|
| `document_id` | Unique Bedesten document ID |
| `title` | Chamber + case/decision numbers |
| `text` | **Full decision text** (extracted from HTML) |
| `date` | Decision date (YYYY-MM-DD) |
| `chamber` | Chamber name (e.g., "12. Hukuk Dairesi") |
| `case_number` | Esas No (e.g., "2025/5612") |
| `decision_number` | Karar No (e.g., "2026/338") |
| `division_type` | civil, criminal, or general_council |

## Notes

- Full text is provided in Turkish (UTF-8)
- Some historical decisions have malformed dates
- Rate limited to 1 request/second to respect the API
- Alternative search portal (karararama.yargitay.gov.tr) requires CAPTCHA

## References

- [Yargıtay Official Website](https://www.yargitay.gov.tr)
- [Mevzuat Portal](https://mevzuat.adalet.gov.tr)
- [Court of Cassation (Wikipedia)](https://en.wikipedia.org/wiki/Court_of_Cassation_(Turkey))

## License

[Open Government Data](https://www.yargitay.gov.tr) — official decisions published by the Court of Cassation of the Republic of Turkey.
