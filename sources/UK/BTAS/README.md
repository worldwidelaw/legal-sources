# UK/BTAS — Bar Tribunals & Adjudication Service (Findings & Sentences)

Full-text findings and sanctions of the disciplinary tribunals convened by the
**Bar Tribunals & Adjudication Service (BTAS)** — the body (run by the Council of
the Inns of Court) that administers the independent tribunals hearing allegations
of professional misconduct against **barristers in England & Wales**, prosecuted
by the Bar Standards Board (BSB).

Tribunal findings and sanctions (disbarment, suspension, prohibition on
practising, fines, reprimand) are quasi-judicial and binding, subject to appeal
to the High Court → `case_law`. BTAS is the barrister-side counterpart of
`UK/SDT` (solicitors), and is distinct from the Bar Standards Board (the
prosecutor/regulator).

## Coverage

~200 tribunal findings (England & Wales, GB-ENG + GB-WLS).

## How it works

1. Page the WordPress listing
   `https://www.tbtas.org.uk/hearings/findings-and-sentences-of-past-hearings/page/{n}/`
   (10 `<article class="listing_item">` hearings/page). Each block's definition
   list carries Defendant (+ Inn), type of hearing, panel members, status, dates
   and the "Finding and sentence" summary linking to the published findings PDF
   under `/wp-content/uploads/hearings/{id}/`.
2. Download the published-findings PDF and extract full text. The PDFs are
   born-digital; extraction uses the shared `common.pdf_extract` markdown
   backend with a **PyMuPDF (fitz) fallback** for environments where the
   markdown backend is unavailable.

## Usage

```bash
python bootstrap.py bootstrap --sample   # sample records for validation
python bootstrap.py bootstrap            # full pull
python bootstrap.py update               # incremental
```

## License

> ⚠️ **Commercial use restricted.** BTAS publishes under its own web terms; no Open Government Licence.

[BTAS web terms & conditions](https://www.tbtas.org.uk/web-terms-conditions/) — custom terms, attribution required, commercial use flagged pending confirmation. The underlying tribunal findings are public records published under the BTAS publication policy.
