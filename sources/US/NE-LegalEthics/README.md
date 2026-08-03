# US/NE-LegalEthics — Nebraska Supreme Court Lawyer Ethics Advisory Opinions

Full text of the **"Nebraska Ethics Advisory Opinion for Lawyers"** series
issued by the **Advisory Committee of the Nebraska Supreme Court** (the
Lawyers' Advisory Committee). Under Nebraska Rule of Discipline 5, the
Committee — whose eight members are appointed by the Supreme Court, one per
judicial district — renders advisory opinions interpreting the Nebraska Rules
of Professional Conduct with respect to a requesting attorney's anticipatory
conduct. These are advisory guidance to **lawyers** = `doctrine`.

- **Publisher:** Nebraska Judicial Branch (Nebraska Supreme Court)
- **Coverage:** ~288 opinions, No. 68-1 (1968) → present
- **Format:** born-digital PDFs (text layer for all eras — no OCR needed)
- **Type:** `doctrine`
- **Jurisdiction:** US-NE

## Access

Server-rendered Drupal site — plain HTTP GET, no auth, JavaScript or CAPTCHA.

1. **Enumeration** — the opinions are listed in a paginated Drupal view:
   `/administration/professional-ethics/attorney-discipline-ethics/lawyer-ethics-opinions?page=N`
   (pages 0–14, ~20 rows/page). Each row (`div.views-row-inner`) carries
   labelled fields: the opinion **Number** (`views-field-title`), the **Year**
   (`views-field-field-year`), the **Question Presented** snippet
   (`views-field-body`) and a **"Download PDF"** link
   (`views-field-field-opinion`). `supremecourt.nebraska.gov` redirects to
   `nebraskajudicial.gov`.
2. **Full text** — each opinion PDF under `/sites/default/files/opinions/` is
   born-digital and extracted with PyMuPDF (`fitz`), no OCR.
3. **Numbering** — filenames are irregular (`25-01.pdf`,
   `Formal-Opinion-%2324-03.pdf`, `Opinion-24-01.pdf`, `68-1_0.pdf`), so the
   href is taken verbatim from the row and the number is read from the row's
   Number field. Numbers are canonicalised to `YYYY-NN` using the
   authoritative Year field (68 → 1968, 25 → 2025).

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction check
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull
```

## Distinct from

- **US/NE-EthicsOpinions** — Nebraska Accountability and Disclosure Commission
  (campaign-finance / conflict-of-interest advice to public **officials**).
- **US/NE-Courts** — Nebraska appellate courts.
- **US/NE-Legislation** — Nebraska Revised Statutes.

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) —
government edict. The Advisory Committee is created by, and its members
appointed by, the Nebraska Supreme Court, and its opinions interpret the
Nebraska Rules of Professional Conduct adopted by the Court. As the work of a
body authorized by the state's highest court, the opinions are treated as
public domain under the government-edicts rationale, consistent with the other
state-court/state-bar legal-ethics sources in this project. Published free to
the public on `nebraskajudicial.gov` with no login, paywall or terms
prohibiting reuse. Commercial use permitted.
