# TR/Danistay — Turkish Council of State

**Source ID:** TR/Danistay
**Country:** Turkey (TR)
**Data Type:** Case Law
**Status:** Complete

## Overview

The Council of State (Danıştay) is Turkey's supreme administrative court. It serves as the court of last instance for administrative law cases and provides opinions on draft legislation submitted by the government.

This scraper fetches precedent-setting decisions (İçtihat Kararları) from the public API, which provides:
- Recent decisions from the Administrative Chambers Board (İDDK)
- Decisions from the Tax Chambers Board (VDDK)
- Unification decisions (İçtihadı Birleştirme Kararları)

## Data Access

### Primary Endpoint
- **API:** `https://api.danistay.gov.tr/api/v1/tr/guncelKararlar`
- **Format:** JSON array with decision metadata and PDF links
- **Coverage:** ~235 recent precedent decisions

### PDF Full Text
- **Base URL:** `https://danistay.gov.tr/assets/pdf/guncelKararlar/{filename}`
- **Format:** PDF documents containing full decision text
- **Extraction:** Uses pdfplumber or PyPDF2 for text extraction

## Decision Types

1. **Unification Decisions (İçtihadı Birleştirme)** — Resolve conflicts between Regional Administrative Courts
2. **Precedent Decisions (Emsal Kararlar)** — Set binding precedent for lower courts
3. **Annulment Decisions (İptal Kararları)** — Cancel administrative acts

## Usage

```bash
# Sample mode (12 records)
python bootstrap.py bootstrap --sample

# Full bootstrap
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## Requirements

PDF text extraction requires one of:
- `pdfplumber` (recommended)
- `PyPDF2`

Install via: `pip install pdfplumber`

## Notes

- The karararama.danistay.gov.tr search portal contains 372,000+ decisions but requires JavaScript/browser rendering
- This scraper accesses only the public API which provides high-value precedent decisions
- All text is in Turkish (UTF-8)
- Rate limited to 1 request/second to respect server limits

## License

[Open Government Data](https://www.danistay.gov.tr) — official decisions published by the Council of State of the Republic of Turkey.
