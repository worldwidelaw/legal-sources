# US/CA-TaxAppeals — California Office of Tax Appeals (Precedential Opinions)

Full text of California's **precedential tax-appeal opinions** published
by the Office of Tax Appeals (OTA) at
[ota.ca.gov/opinions](https://ota.ca.gov/opinions/).

The corpus is dominated by the legacy **State Board of Equalization
(SBE)** precedential opinions — which remain binding precedent before the
OTA — plus OTA's own precedential opinions. Each opinion resolves a tax
controversy between a taxpayer/appellant and the Franchise Tax Board or
the California Department of Tax and Fee Administration (CDTFA), so the
corpus is `case_law`.

## Source

The single `/opinions/` page is a server-rendered **TablePress** listing:
every opinion row and its `wp-content/uploads/.../*.pdf` link is present
in the HTML, and the DataTables widget only paginates client-side. About
**4,000 opinion PDFs** are listed. No JavaScript, no CAPTCHA, no auth.

## How it works

1. GET the `/opinions/` HTML page (one request).
2. Collect every opinion PDF link, skip admin docs (org charts, errata
   notices, agendas), and dedup by URL.
3. Parse the opinion number (`{YY}-SBE-{NNN}` / `{YY}-OTA-{NNN}`) and
   year from the filename.
4. Download each PDF and extract its text layer via
   `common.pdf_extract`.
5. Derive the appellant name and decision date from the document text
   (pleading line-numbers are stripped; the date prefers the
   "this Nth day of Month, YYYY" clause).
6. Normalize into the standard `case_law` schema.

## Usage

```bash
python bootstrap.py bootstrap            # Full pull (all opinions)
python bootstrap.py bootstrap --sample   # ~12 sample documents
python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
python bootstrap.py test-api             # Connectivity test
```

## License

[Public Domain (US Government Work — California)](https://www.law.cornell.edu/uscode/text/17/105) — California Office of Tax Appeals / State Board of Equalization precedential opinions are official state government works in the public domain. No attribution required; commercial use permitted.
