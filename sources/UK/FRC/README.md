# UK/FRC — Financial Reporting Council: Enforcement & Tribunal Decisions

Full-text enforcement and disciplinary tribunal decisions of the UK **Financial
Reporting Council (FRC)** against statutory auditors, audit firms, accountants
and actuaries.

Cases are pursued under three regimes:

- the **Audit Enforcement Procedure (AEP)** — Final Settlement Decision Notices
  and Executive Counsel Final Decision Notices;
- the **Accountancy Scheme** and **Actuarial Scheme** — Disciplinary Tribunal
  Reports and settlement agreements.

Each concluded case publishes a reasoned decision document setting out the
facts, the misconduct / breaches found, the sanctions imposed (financial
penalties, exclusion / removal, reprimand, non-audit undertakings) and the
reasons. These are binding professional-enforcement adjudications = **case law**.

## Access & structure

All public, no authentication:

- Enforcement outcomes are indexed on the server-rendered CMS pages:
  - `/library/enforcement/enforcement-cases/` (all cases)
  - `/library/enforcement/accountancy-scheme/`
  - `/library/enforcement/actuarial-scheme/`
  - `/library/enforcement/audit-enforcement-procedure/`
- Each index links per-case outcome pages under
  `/news-and-events/news/{yr}/{mo}/{slug}/`, which carry the case title, a
  `Published: {date}` line and a link to the decision document.
- Decision documents are **born-digital PDFs** at `/documents/{id}/{name}.pdf`
  (302-redirecting to `media.frc.org.uk`), with a real text layer extracted via
  PyMuPDF — no OCR required. The stable document id is the record id.

## Usage

```bash
python bootstrap.py bootstrap          # Full pull
python bootstrap.py bootstrap --sample # Sample records for validation
python bootstrap.py bootstrap-fast     # Full pull (runner alias)
python bootstrap.py update             # Incremental (recent published first)
python bootstrap.py test               # Quick connectivity test
```

## Data

- Full-text FRC enforcement / disciplinary tribunal decisions (~200 cases in the
  published index; decision PDFs range ~7k–200k characters).
- Language: English. Jurisdiction: GB. Auth: none.

## License

> ⚠️ **Commercial use restricted.** The FRC is a company limited by guarantee
> (Registered in England no. 2486368), not a Crown body — no Open Government
> Licence applies. The website terms of use restrict reproduction, republishing
> and creating derivative works absent permission.

[Disclaimer and Copyright — © The Financial Reporting Council Limited](https://www.frc.org.uk/about-us/policies-and-procedures/disclaimer-and-copyright/) — attribution required; commercial reuse restricted. Decision documents are published by the FRC for transparency of its enforcement outcomes.
