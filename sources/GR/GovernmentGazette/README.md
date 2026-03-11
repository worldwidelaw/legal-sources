# GR/GovernmentGazette - Greek Government Gazette (FEK)

Official Gazette of the Hellenic Republic (Εφημερίς της Κυβερνήσεως - FEK).

## Overview

All Greek laws, presidential decrees, and regulatory acts must be published in the Government Gazette (FEK) before they enter into force. This is Greece's primary legislation publication channel.

**Note:** Greece does NOT have a free consolidated legislation database. The FEK contains the original enacted text of all laws. For consolidated/amended versions, commercial databases like NOMOS (lawdb.intrasoftnet.com) require subscription.

## Data Source

- **URL:** https://www.et.gr
- **API:** PDF download via `/api/DownloadFeksApi/?fek_pdf={code}`
- **Format:** PDF documents
- **Language:** Greek
- **License:** Public Domain (Official Government Acts)

## FEK Code Format

FEK codes follow the pattern: `YYYYSSNNNN`

- **YYYY:** Year (e.g., 2024)
- **SS:** Series code (2 digits)
- **NNNN:** Issue number (5 digits, zero-padded)

### Series Codes

| Code | Greek Name | English | Content |
|------|------------|---------|---------|
| 01 | ΤΕΥΧΟΣ ΠΡΩΤΟ | First Volume | Laws, Presidential Decrees |
| 02 | ΤΕΥΧΟΣ ΔΕΥΤΕΡΟ | Second Volume | Regulatory Acts, Ministerial Decisions |
| 03 | ΤΕΥΧΟΣ ΤΡΙΤΟ | Third Volume | Appointments |
| 04 | ΤΕΥΧΟΣ ΤΕΤΑΡΤΟ | Fourth Volume | Announcements, Competitions |
| 10 | ΤΕΥΧΟΣ Α.Σ.Ε.Π. | ASEP Volume | Civil Service Examinations |

**Primary legislation** (laws, decrees) is in Series 01 (ΤΕΥΧΟΣ ΠΡΩΤΟ).

## Usage

```bash
# Test API connection
python bootstrap.py test

# Fetch sample records for validation
python bootstrap.py bootstrap --sample

# Fetch updates from last 30 days
python bootstrap.py update
```

## Technical Notes

- Uses IP address (20.95.103.179) with Host header due to redirect behavior
- Requires `pdfplumber` for PDF text extraction
- SSL verification disabled due to certificate chain issues
- Rate limited to 2 requests/second

## Sample Output

```json
{
  "_id": "20240100061",
  "_source": "GR/GovernmentGazette",
  "_type": "legislation",
  "title": "ΝΟΜΟΣ ΥΠ' ΑΡΙΘΜ. 5105 - Δημιουργική Ελλάδα...",
  "text": "ΕΦΗΜΕΡΙ∆Α ΤΗΣ ΚΥΒΕΡΝΗΣΕΩΣ...",
  "date": "2024-04-29T00:00:00+00:00",
  "fek_code": "20240100061",
  "fek_year": 2024,
  "fek_series": "01",
  "fek_series_name": "ΤΕΥΧΟΣ ΠΡΩΤΟ",
  "fek_issue": 61
}
```

## Related Sources

- **GR/Diavgeia:** Administrative decisions (71M+ records) - complements this source
- **GR/SupremeCourt:** Case law from Areios Pagos
