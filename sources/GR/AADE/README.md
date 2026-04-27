# GR/AADE - Greek Tax Authority (AADE)

**Country:** Greece (GR)
**Data Type:** Doctrine
**Status:** Complete

## Overview

This source fetches tax circulars and official interpretations from the Greek Independent Authority for Public Revenue (Ανεξάρτητη Αρχή Δημοσίων Εσόδων - AADE).

## Data Source

AADE publishes all official documents through the Greek government transparency portal **Diavgeia** (Διαύγεια). This source uses the Diavgeia OpenData API to fetch AADE-specific doctrine documents:

- **Circulars (Εγκύκλιοι)** - Type Α.3: Official interpretations of tax law
- **Opinions (Γνωμοδοτήσεις)** - Type Α.4: Advisory opinions on tax matters

## API Details

- **Base URL:** https://diavgeia.gov.gr/luminapi/opendata
- **Organization UID:** 100029495
- **Document format:** PDF (full text extracted)
- **License:** [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)

## Coverage

- **Start Date:** 2016 (when AADE was established)
- **Total Documents:** ~50 circulars and opinions
- **Update Frequency:** Weekly (new circulars published regularly)

## Usage

```bash
# Test API connectivity
python bootstrap.py test

# Fetch sample records for validation
python bootstrap.py bootstrap --sample

# Full bootstrap (fetch all documents)
python bootstrap.py bootstrap

# Incremental update (last 7 days)
python bootstrap.py update
```

## Schema

Key fields in normalized records:

| Field | Description |
|-------|-------------|
| `_id` | ADA (unique document identifier) |
| `title` | Circular name + subject |
| `text` | Full text content (extracted from PDF) |
| `date` | Issue date |
| `circular_name` | Circular reference (e.g., "Ε.2012") |
| `protocol_number` | Protocol number |
| `decision_type` | Α.3 (circular) or Α.4 (opinion) |

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — published via [Diavgeia](https://diavgeia.gov.gr).

## Notes

- AADE circulars provide authoritative interpretations of Greek tax law
- All documents are in Greek
- Full text is extracted from PDF documents using pdfplumber
