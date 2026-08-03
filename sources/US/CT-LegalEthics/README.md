# US/CT-LegalEthics — Connecticut Bar Association — Informal Ethics Opinions

Full text of the **Informal Opinions** issued by the **Connecticut Bar
Association's Standing Committee on Professional Ethics** (now the Committee on
Professional Ethics and the Unauthorized Practice of Law). Each opinion is the
Committee's written response to a member's inquiry, interpreting the
**Connecticut Rules of Professional Conduct** to advise **lawyers** — advisory
(no weight of law) = **doctrine**.

- **Publisher:** Connecticut Bar Association (CBA), New Britain/Meriden, CT
- **Coverage:** one numbered series `YY-NN`, **~74 opinions, 2011–present**
- **Format:** born-digital PDFs (text layer) — extracted with PyMuPDF, **no OCR**
- **Jurisdiction:** US-CT (Connecticut)

This is the attorney professional-conduct advisory-opinion series that in other
states we build as `US/{ST}-LegalEthics`. It is **distinct** from:
- `US/CT-EthicsOpinions` — the executive **Connecticut Office of State Ethics**
  (public officials), and
- `US/CT-Courts`.

## Access

1. The opinions are listed on the CBA public page
   `https://www.ctbar.org/news/CBAPublications/informal-ethics-opinions`.
   Each is a direct PDF link whose **anchor text** is
   `Informal Opinion {YY-NN} | {Title}` — the opinion number and title come
   straight from the index. The year folder in the PDF path
   (`/…/{YYYY}-opinions/…` or `/…/{YYYY}/…`) gives the authoritative 4-digit
   year, used to canonicalise the number to `YYYY-NN`.
2. Each opinion PDF is born-digital; PyMuPDF (`fitz`) extracts the full text
   directly. A handful of older PDFs carry stray control-char artifacts
   (`\x01` form markers) which are stripped during cleaning.
3. `date` is taken from the `Approved Month DD, YYYY` / issue-date line in the
   header region (ordinals like `19th` allowed), with a ±1-year guard; it falls
   back to `YYYY-01-01`.

No JavaScript execution, CAPTCHA, or authentication is required.

**Scope:** only the free 2011-present corpus on ctbar.org is captured. Formal
Opinions and pre-2011 Informal Opinions (back to Connecticut's 1986 adoption of
the Rules of Professional Conduct) are available only through the paywalled
Casemaker / vLex service and are excluded.

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction test
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull (all opinions)
python bootstrap.py bootstrap-fast      # alias for full pull (VPS wrapper)
```

## Record schema

| field | description |
|-------|-------------|
| `_id` | `US/CT-LegalEthics/{opinion_number}` |
| `_source` | `US/CT-LegalEthics` |
| `_type` | `doctrine` |
| `opinion_number` | canonical `YYYY-NN` (e.g. `2011-01`, `2025-04`) |
| `title` | opinion title (from the CBA index anchor text) |
| `text` | full opinion text (born-digital PDF, no OCR) |
| `date` | approved/issue date, ISO 8601 |
| `issuer` | Connecticut Bar Association — Committee on Professional Ethics |
| `jurisdiction` | `US-CT` |
| `url` | link to the original PDF |

## License

[Public Domain / freely published advisory opinions](https://www.ctbar.org/news/CBAPublications/informal-ethics-opinions)
— CBA Informal Ethics Opinions are published free to the public on ctbar.org as
an educational service interpreting the Connecticut Rules of Professional
Conduct. They are advisory (no weight of law) and carry no login, paywall or
terms prohibiting reuse. Treated as effectively public domain, consistent with
the other state-bar legal-ethics sources (`US/NY-LegalEthics`,
`US/IL-LegalEthics`). Note: the CBA is a **voluntary** bar association (not an
integrated/state bar), so the 17 U.S.C. § 105 government-edicts rationale is
weaker than for court-arm boards; flagged here as freely-published advisory
material with **commercial use permitted**.
