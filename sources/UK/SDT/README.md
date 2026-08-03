# UK/SDT — Solicitors Disciplinary Tribunal (Judgments)

Full-text judgments of the **Solicitors Disciplinary Tribunal (SDT)**, the
independent statutory tribunal constituted under s.46 of the Solicitors Act 1974.
The SDT adjudicates allegations of professional misconduct against solicitors,
registered European lawyers (RELs), registered foreign lawyers (RFLs), and other
persons regulated by the Solicitors Regulation Authority in England & Wales. Its
orders (strike-off from the Roll, suspension, fine, restrictions, reprimand, or no
order) are quasi-judicial and binding, subject to appeal only to the High Court
(Administrative Court) — hence `case_law`.

The SDT is **distinct from UK/SRA**: the SRA (Solicitors Regulation Authority)
investigates and prosecutes; the SDT is the independent judicial body that hears
the case and decides.

## Data source

- **Cases archive:** `https://solicitorstribunal.org.uk/case/page/{n}/`
  — a paginated WordPress listing (~10 cases per page) of per-case pages.
- **Per-case page:** `https://solicitorstribunal.org.uk/case/{id}/`
  — carries structured metadata (Case ID, SRA ID, Year, Publication date,
  Applicant, Respondent, Allegation, Outcome, Executive summary) and a link to
  the full **Final Judgment PDF** under `/wp-content/uploads/`.
- **Full text:** the Final Judgment PDFs are born-digital (text layer) and are
  extracted in full via the shared `common.pdf_extract` (no OCR). The PDF host
  rejects the plain `requests` User-Agent (HTTP 403), so the scraper downloads
  the bytes with a browser User-Agent and passes them to the extractor.

Coverage: ~2,200 judgments (the online archive runs 2016–present).

## Usage

```bash
python bootstrap.py bootstrap          # Full initial pull
python bootstrap.py bootstrap --sample # 12 sample records for validation
python bootstrap.py bootstrap-fast     # Alias for full pull (fleet runner)
```

## License

> ⚠️ **Commercial use restricted.** The SDT website asserts site copyright with
> separate Terms & Conditions and carries no Open Government Licence statement.

[Terms & Conditions](https://solicitorstribunal.org.uk/terms/) — SDT judgments are
public documents published under the Tribunal's judgment publication policy, but
re-use terms are not open. Commercial use is flagged pending confirmation;
attribution to the Solicitors Disciplinary Tribunal is expected.
