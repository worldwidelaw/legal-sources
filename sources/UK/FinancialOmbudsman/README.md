# UK/FinancialOmbudsman — Financial Ombudsman Service (FOS) — Ombudsman Decisions

Final, binding decisions of the **Financial Ombudsman Service**, the UK's
statutory dispute-resolution scheme for financial services, established under
Part XVI of the **Financial Services and Markets Act 2000 (FSMA)**.

When a consumer complaint against a financial business cannot be settled, an
Ombudsman issues a final decision determining the complaint (**upheld** / **not
upheld**) and, where upheld, directing redress. Since **1 April 2013** the FCA's
DISP rules require FOS to publish every final decision; the Service publishes
them, anonymised, on its website as born-digital PDFs.

Each decision is an adjudication of a specific dispute by a statutory
office-holder → **case_law**. The corpus is one of the largest single-tribunal
bodies of published decisions anywhere: **404,000+ final decisions** (404,560
reported at build time) covering banking, insurance, mortgages, investments,
pensions, consumer credit, PPI, payment protection and fraud/scams.

## Data

- **Type:** case_law (UK / GB)
- **Coverage:** 404,000+ final decisions, 2013–present
- **Language:** English
- **Auth:** none (free public access)
- **Full text:** yes — born-digital decision PDFs (PyMuPDF; no OCR needed)

## How it works

1. The public **Ombudsman decisions** search
   (`/businesses/resolving-complaint/ombudsman-decisions/search`) is a
   server-rendered listing: 10 results per page, paged with `?Start=N` (N steps
   by 10), `Sort=date`. Each result carries the Decision Reference
   (`DRN-NNNNNNN`), decision date, business name, outcome and a link to the
   decision PDF at `/decision/{DRN}.pdf`.
2. Page the listing to enumerate every DRN and its listing metadata.
3. Download each born-digital decision PDF and extract the full text with
   PyMuPDF (pdfplumber/pypdf fallback).
4. One normalized record per decision.

## Usage

```bash
python bootstrap.py test               # connectivity + parse + one PDF
python bootstrap.py bootstrap --sample # 15 sample records for validation
python bootstrap.py bootstrap          # full pull
python bootstrap.py bootstrap-fast     # full pull (runner alias)
python bootstrap.py update             # incremental (recent decisions)
```

## License

> ⚠️ **Commercial use restricted.** See note below.

[© Financial Ombudsman Service — website terms](https://www.financial-ombudsman.org.uk/) — Published decisions are copyright of the Financial Ombudsman Service. FOS is a statutory ombudsman (a company limited by guarantee) established under FSMA 2000, **not** a Crown body, and publishes **no** explicit Open Government Licence or Creative Commons grant covering the decision corpus. Commercial re-use is therefore not clearly permitted and is **flagged** per project policy. The decisions are public records, free to access with no login or paywall, and `robots.txt` permits crawling.
