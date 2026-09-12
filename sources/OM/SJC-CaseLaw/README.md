# OM/SJC-CaseLaw — Oman Supreme Court Legal Principles

Official compilations of legal principles (المبادئ القانونية) endorsed by the
**Supreme Court of Oman** (المحكمة العليا), published by the Technical Bureau of the
**Supreme Judiciary Council** (المجلس الأعلى للقضاء / sjc.gov.om).

Each volume gathers the legal principles / ratio decidendi extracted from Supreme
Court judgments, organised by **year** and by **division**:

- الدائرة الجزائية — Criminal
- الدائرة الشرعية والمدنية — Personal status & Civil
- الدائرة العمالية والتجارية والإيجارات — Labour, Commercial & Rental
- الديات والأروش — Blood-money (diya) & Compensation (arsh)
- A bilingual **Arabic/English** "Selected Collection of Legal Principles Endorsed by
  the Supreme Court" volume

Coverage: **2011–2025**. This is the **first case-law source for Oman** in the corpus.

## Access

- Listing page (server-side HTML, no browser required):
  `https://www.sjc.gov.om/InnerPage.aspx?ID=2095bd1d-1eca-4996-90e0-76652101f3c3`
- PDF volumes are static, born-digital files under `/userupload/ Legal principles/`.
- Full text is extracted with **PyMuPDF (fitz)** and normalised with Unicode **NFKC**
  (maps Arabic presentation-forms to standard Arabic letters).
- One normalized record per published volume (~19 volumes).

### Notes / gotchas

- Arabic PDF filenames on the server use a fixed Unicode normalisation; always fetch
  the **exact href bytes** parsed from the page HTML (do not retype the Arabic names)
  and URL-encode with `urllib.parse.quote`.
- The host returns intermittent `404`/timeouts — requests retry with backoff.
- Decorative **cover/title** fonts have broken cmaps and extract as garbage, but the
  **body text is clean Arabic** (Arabic-block ratio ≈ 1.0 of alphabetic characters).

No authentication. No API required.

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction check
python bootstrap.py bootstrap --sample   # save sample records
python bootstrap.py bootstrap            # full pull
python bootstrap.py bootstrap-fast       # alias for full pull (fleet)
```

## License

[Public Domain (Government)](https://www.sjc.gov.om/) — official Supreme Court judicial
principles published by the Omani Supreme Judiciary Council. Official legal texts and
court judgments are not subject to copyright. Commercial use permitted.
