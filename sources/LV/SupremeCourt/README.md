# LV/SupremeCourt - Latvian Supreme Court (Senāts)

## Overview

This source fetches case law decisions from the Latvian Supreme Court (Augstākās tiesas Senāts) via the National Courts Portal (manas.tiesas.lv).

## Data Access

- **Portal**: https://manas.tiesas.lv/eTiesasMvc/nolemumi
- **Auth**: Public access (no authentication required)
- **Format**: PDF documents with ECLI metadata (JSON)
- **Coverage**: Decisions from 2017 onwards (ECLI implementation date)

## Endpoints

| Endpoint | Description |
|----------|-------------|
| `/eTiesasMvc/lv/nolemumi` | Search decisions (POST form) |
| `/eTiesasMvc/geteclimetadata/{ECLI}` | JSON metadata for a decision |
| `/eTiesasMvc/nolemumi/pdf/{id}.pdf` | Download anonymized PDF |

## ECLI Format

Latvia uses the European Case Law Identifier (ECLI) since September 2017:

```
ECLI:LV:AT:2025:0320.C30738321.17.S
      |  |  |    |    |           |
      |  |  |    |    |           +-- Type: S=Spriedums, L=Lēmums
      |  |  |    |    +-------------- Case number + sequence
      |  |  |    +------------------- Date: MMDD
      |  |  +------------------------ Year
      |  +--------------------------- Court: AT=Supreme Court
      +------------------------------ Country: LV=Latvia
```

## Court ID

- **44**: Augstākās tiesas Senāts (Supreme Court Senate)
- **45**: Augstākās tiesas Tiesu palāta (Supreme Court Chamber)

## Data Fields

| Field | Description |
|-------|-------------|
| `ecli` | European Case Law Identifier |
| `title` | Case type (Civillieta, Krimināllieta, etc.) |
| `text` | Full text extracted from PDF |
| `date` | Decision date (YYYY-MM-DD) |
| `court` | Court name |
| `judges` | Judge name(s) |
| `url` | Link to PDF document |

## Usage

```bash
# Quick connectivity test
python bootstrap.py test

# Fetch sample data (10+ records)
python bootstrap.py bootstrap --sample

# Full bootstrap (all decisions since 2017)
python bootstrap.py bootstrap

# Incremental update (recent decisions)
python bootstrap.py update
```

## Dependencies

- `requests`: HTTP client
- `PyPDF2` or `pypdf`: PDF text extraction

## License

Open government data — Latvian court decisions are published via the [National Courts Portal](https://manas.tiesas.lv) and freely reusable.

## Notes

- Decisions are anonymized (personal data redacted)
- Full text in Latvian language
- Includes Civil, Criminal, and Administrative departments
- Rate limited to 30 requests/minute
