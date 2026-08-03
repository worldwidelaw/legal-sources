# IE/AnBordPleanala — An Bord Pleanála / An Coimisiún Pleanála (Planning Appeal Decisions)

**An Bord Pleanála** (renamed **An Coimisiún Pleanála** in 2024) is Ireland's
national independent statutory body that decides appeals, referrals and direct
applications on planning, strategic infrastructure and related consents under the
Planning and Development Act 2000. Its determinations are binding public
adjudications = **case_law**.

For every determined case the Board publishes the reasoned **Inspector's Report**
— a born-digital PDF with a full text layer — alongside the short formal Board
Order and Board Direction (scanned images). The Inspector's Report carries the
substantive reasoning (planning history, submissions, assessment, recommendation)
and is captured here as the full text.

## Coverage

- ~20,000+ determined cases with Inspector's Reports, **2016–present**
- Language: English
- Auth: none (free public access)

## Access

1. **Case listing**, windowed by decision date (the listing caps at 500 results,
   so monthly windows are used and any window that hits the cap is recursively
   split):
   ```
   GET /en-ie/cases?decisionFrom=YYYY-MM-DD&decisionTo=YYYY-MM-DD
   → anchors /en-ie/case/{N}
   ```
2. **Case detail** page — parsed for metadata (case reference, planning authority
   reference, site address, case type, decision, date signed) and the Inspector's
   Report PDF link:
   ```
   GET /en-ie/case/{N}
   ```
3. **Inspector's Report** PDF (born-digital, text via PyMuPDF/fitz):
   ```
   /anbordpleanala/media/abp/cases/reports/{N[:3]}/r{N}.pdf
   ```
   Cases whose report is missing or scanned (no text layer) are skipped rather
   than stored metadata-only.

## Usage

```bash
python bootstrap.py bootstrap          # Full pull
python bootstrap.py bootstrap --sample # 15 sample records for validation
python bootstrap.py bootstrap-fast     # Full pull (runner alias)
python bootstrap.py update             # Incremental (recent months)
python bootstrap.py test               # Quick connectivity test
```

## License

[PSI Licence / CC BY 4.0](https://www.gov.ie/en/help/re-use-of-public-sector-information/)
— Irish public sector information re-use framework (default CC BY 4.0). Attribution
required. Commercial use permitted.
