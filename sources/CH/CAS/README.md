# CH/CAS - Court of Arbitration for Sport

## Overview

This source fetches arbitration awards from the Court of Arbitration for Sport (CAS / TAS), headquartered in Lausanne, Switzerland.

The CAS is the supreme court for international sports disputes. It provides final, binding arbitration for disputes involving international sports federations, athletes, and sports-related organizations.

## Data Source

- **API**: `https://jurisprudence.tas-cas.org/CaseLawDocument/SearchCaseLawDocument`
- **PDFs**: `https://jurisprudence.tas-cas.org/pdf/{filename}`
- **Total awards**: ~2,600 (non-confidential awards since 1986)

## Data Types

The CAS publishes several types of procedures:

| Code | Type | Description |
|------|------|-------------|
| A | Appeal Procedure | Appeals against decisions of sports federations |
| O | Ordinary Procedure | First-instance disputes submitted directly to CAS |
| AHD | Ad Hoc Division | Urgent decisions during major sporting events |
| ADD | Anti-Doping Division | Doping-related disputes |

## Usage

```bash
# Fetch sample (15 records with full text)
python bootstrap.py bootstrap --sample

# Fetch all awards
python bootstrap.py bootstrap --full
```

## Output Schema

| Field | Description |
|-------|-------------|
| `_id` | Unique identifier (e.g., "CH/CAS/2023_A_10168") |
| `title` | Case number (e.g., "2023/A/10168") |
| `text` | Full text of the arbitration award |
| `date` | Decision date |
| `case_number` | CAS case number |
| `procedure_type` | Type of procedure (Appeal, Ordinary, etc.) |
| `matter_type` | Subject matter (Disciplinary, Doping, Transfer, etc.) |
| `outcome` | Decision outcome (Upheld, Dismissed, Partially Upheld, etc.) |
| `sport` | Sport involved (Football, Athletics, etc.) |
| `appellants` | Appealing parties |
| `respondents` | Responding parties |
| `arbitrators` | Panel members |
| `keywords` | Legal keywords |

## License

> ⚠️ **Commercial use unclear.** CAS awards are published for public information and legal research. Verify terms before commercial redistribution.

[CAS Jurisprudence](https://jurisprudence.tas-cas.org/) — texts are specifically formatted for the jurisprudence database.

## Notes

- Only non-confidential awards are published
- Awards since August 2012 include paragraph numbering from the original
- Full text is extracted from PDF documents using pdfplumber
