# UK/PensionsOmbudsman — The Pensions Ombudsman (TPO) Determinations

Published determinations of **The Pensions Ombudsman (TPO)**, the statutory
tribunal established under the Pension Schemes Act 1993 that investigates and
decides complaints and disputes concerning the administration and management of
occupational and personal pension schemes.

TPO determinations are **final and binding** on the parties (enforceable in the
county court) and appealable only on a point of law to the High Court — i.e.
`case_law`.

## Coverage

- ~6,700 determinations, 1998–present (Great Britain).
- ~3,000 modern determinations are **born-digital PDFs** and extract cleanly.
- Some middle-era determinations are `.docx` (extracted with `python-docx`).
- Legacy binary `.doc` files (oldest cases) have no reliable text extractor
  available and are **skipped** (logged), so only full-text records are emitted.

## How it works

1. The decisions listing is a paginated Drupal archive
   (`/decisions?page={n}`, 12 cards per page, ~565 pages). Each card carries the
   full metadata: title, complainant, respondent, outcome, complaint topic,
   case reference and decision date.
2. Each decision has its own page `/decision/{year}/{ref}/{slug}` which links to
   the determination file under `/sites/default/files/decisions/`.
3. The determination file (PDF / `.docx`) is downloaded and its full text
   extracted (shared `common.pdf_extract` for PDFs — born-digital, no OCR).

## Usage

```bash
python bootstrap.py bootstrap          # Full pull
python bootstrap.py bootstrap --sample # 12 sample records
python bootstrap.py bootstrap-fast     # Alias for full pull (fleet runner)
```

## License

> ⚠️ **Commercial use flagged.** The site carries no explicit Open Government
> Licence statement; the footer asserts "© The Pensions Ombudsman". Treated as
> custom terms pending confirmation.

[Terms and Conditions](https://www.pensions-ombudsman.org.uk/terms-and-conditions)
— © The Pensions Ombudsman. Determinations are public official documents;
reuse terms not explicitly stated (no OGL grant found), so commercial use is
flagged.
