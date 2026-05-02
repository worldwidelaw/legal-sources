# CN/SAMR-Penalties

**China State Administration for Market Regulation (SAMR) — Administrative Penalty Decisions**

Administrative penalty decisions published by SAMR and its local market supervision bureaus across China. Covers food safety violations, antitrust enforcement, consumer protection, advertising violations, and market regulation. ~709,000+ decisions with full text.

- **Source:** https://cfws.samr.gov.cn/
- **Type:** case_law
- **Language:** Chinese (Simplified)
- **Coverage:** 2018–present
- **Records:** ~709,000+
- **Full text:** Yes (extracted from base64-encoded PDFs via API)

## Usage

```bash
# Test API connectivity
python bootstrap.py test-api

# Fetch sample records (15 documents)
python bootstrap.py bootstrap --sample

# Full bootstrap (all documents, iterates monthly date ranges)
python bootstrap.py bootstrap
```

## Dependencies

- `requests`
- `pycryptodome` (for DES3 anti-bot cipher)
- `pdfplumber` or `pypdf` (for PDF text extraction)

## License

> ⚠️ **Commercial use restricted.** See terms below.

[SAMR Public Disclosure Terms](https://cfws.samr.gov.cn/) — Site footer indicates non-commercial use restriction. Government penalty decisions are publicly disclosed per PRC administrative transparency requirements.
