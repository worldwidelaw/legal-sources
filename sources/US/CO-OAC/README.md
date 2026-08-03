# US/CO-OAC — Colorado Office of Administrative Courts (Workers' Compensation Decisions)

Full text of Administrative Law Judge decisions and orders of the **Colorado
Office of Administrative Courts (OAC)** — the state's central, independent
administrative tribunal — Workers' Compensation unit. Each decision resolves a
specific contested workers'-compensation claim between an injured worker and an
employer/insurer, so each is **case_law**.

## Source

- **Listing:** https://oac.colorado.gov/case-types/workers-compensation/decisions-and-orders-wc
- **Documents:** born-digital compilation PDFs at
  `https://oac.colorado.gov/sites/oac/files/documents/{compilation}.pdf`
- **Coverage:** 2015–present (~34 monthly/annual compilation PDFs, each holding
  many individual decisions — hundreds/thousands of decisions in total)
- **Access:** no JavaScript, no CAPTCHA, no auth

## How it works

1. `discover_documents()` fetches the listing page and harvests every
   compilation PDF link (`/sites/oac/files/documents/*.pdf`), inferring a
   period label (year + month) from each file name.
2. For each PDF, `_process_pdf()` downloads it (curl, browser UA, ~1 req/s) and
   extracts the text layer via `common.pdf_extract`.
3. `_split_decisions()` splits the compilation on the fixed per-decision header
   — `Office of Administrative Courts / State of Colorado / Workers' Compensation
   No. WC ...` — emitting **one record per individual decision** (WC number,
   signing date, body). If no header is found the compilation is kept whole so
   no full text is lost.

`_id` = `US/CO-OAC/{wc-number}-{sha1(text)[:10]}`; the text hash dedups
identical re-postings and disambiguates a WC number that recurs across months.

## Usage

```bash
python bootstrap.py test-api            # connectivity + split smoke test
python bootstrap.py bootstrap --sample  # ~12 sample decisions
python bootstrap.py bootstrap           # full pull
python bootstrap.py bootstrap-fast      # alias for full pull (VPS wrapper)
```

## License

[Public Domain — 17 U.S.C. § 105 (US government work)](https://www.law.cornell.edu/uscode/text/17/105) — decisions of the Colorado Office of Administrative Courts are official Colorado state-government works in the public domain under the government-edicts doctrine. Commercial use permitted; no attribution required.
