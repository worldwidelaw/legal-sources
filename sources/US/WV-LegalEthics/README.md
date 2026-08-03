# US/WV-LegalEthics — West Virginia Legal Ethics Opinions

Official **Legal Ethics Opinions (L.E.O.)** issued by the **Lawyer Disciplinary
Board** of the West Virginia Office of Disciplinary Counsel — an arm of the
Supreme Court of Appeals of West Virginia. The opinions interpret the West
Virginia Rules of Professional Conduct and advise members of the Bar on the
ethical propriety of contemplated conduct.

- **Type:** doctrine (official government advisory opinion)
- **Coverage:** mid-1970s "Legal Ethics Inquiry" numbers (76-1 …) through the
  modern "L.E.O. YYYY-NN" numbering (e.g. L.E.O. 2024-01, Artificial
  Intelligence). ~110 opinions.
- **Source:** <https://wvodc.org/Legal-Ethics-Opinion>

## Access

The opinions are listed on a single public HTML page. Each opinion is an anchor
whose visible text carries the number and subject (e.g. "L.E.O. 2024-01
Artificial Intelligence" or "76-1 EMPLOYMENT — DUAL PRACTICE OF LAW …"). The
href points to a **scanned PDF** hosted on `storage.googleapis.com`. The PDFs
are image-only (no text layer), so full text is recovered by **OCR** via the
shared `common.pdf_extract` helper (text-layer backends fall through to
tesseract automatically). No JavaScript, CAPTCHA, or authentication.

## Usage

```bash
python bootstrap.py bootstrap --sample   # ~12 samples
python bootstrap.py bootstrap            # full corpus
python bootstrap.py test-api             # connectivity + extraction check
```

OCR requires the `tesseract` binary on `PATH` (present on the fleet VPS).

## Distinct from

- **US/WV-EthicsOpinions** — West Virginia Ethics Commission advisory opinions
  for public officials/employees (executive-branch conflicts of interest).
- **US/WV-Courts**, **US/WV-Legislation**, **US/WV-COMAR**.

## License

[Public Domain (US government edict)](https://www.law.cornell.edu/uscode/text/17/105) —
official advisory opinions of a West Virginia government body (the Lawyer
Disciplinary Board of the Supreme Court of Appeals of West Virginia). Edicts of
government are in the public domain and not subject to copyright. Commercial use
permitted; no attribution required.
