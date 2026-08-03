# US/VT-LegalEthics — Vermont Bar Association — Advisory Ethics Opinions

Full text of the **Advisory Ethics Opinions** issued by the **Vermont Bar
Association's Professional Responsibility Committee** (formerly the Committee on
Professional Ethics). Each opinion is the Committee's advisory answer to a
member's inquiry, interpreting the **Vermont Rules of Professional Conduct**
(formerly the Code of Professional Responsibility) to advise **lawyers** —
advisory (no weight of law) = **doctrine**.

- **Publisher:** Vermont Bar Association (VBA), Montpelier, VT
- **Coverage:** one numbered series `YY-NN`, **~312 opinions, 1978–present**
- **Format:** born-digital PDFs (text layer) — extracted with PyMuPDF, **no OCR**
- **Jurisdiction:** US-VT (Vermont)

This is the attorney professional-conduct advisory-opinion series that in other
states we build as `US/{ST}-LegalEthics`. Distinct from `US/VT-Legislation` and
Vermont judicial/AG sources.

## Access

1. The opinions are published under `/advisory_ethics/`, a WordPress
   custom-post-type archive organised by **topic (category) pages**. The archive
   index paginates at `/advisory_ethics/page/{n}/`; each entry links to a topic
   page `/advisory_ethics/{topic}/` (~70 topics).
2. Each topic page lists that topic's opinions as direct born-digital PDF links
   `/wp-content/uploads/{YYYY}/{MM}/{YY-NN}[-v].pdf`. The same opinion appears
   under several topics, so opinions are **de-duplicated on the canonical
   number** (first `/wp-content/` URL wins).
3. The site's `wp-json` REST API is WAF-403 (returns an HTML block page), so the
   archive HTML is scraped directly with a browser UA.
4. Each opinion PDF is born-digital; PyMuPDF (`fitz`) extracts the full text
   directly (`ADVISORY ETHICS OPINION {num}` / `OPINION {num}`, a `SYNOPSIS`
   summary and full discussion).

No JavaScript execution, CAPTCHA, or authentication is required.

**Numbering:** canonicalised from the PDF filename `YY-NN` (`YY>=78` → `19YY`,
else `20YY`). WordPress upload-collision suffixes (`-1`, `-2`, …) on re-uploaded
files are stripped — they denote the same opinion listed under multiple topics.

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
| `_id` | `US/VT-LegalEthics/{opinion_number}` |
| `_source` | `US/VT-LegalEthics` |
| `_type` | `doctrine` |
| `opinion_number` | canonical `YYYY-NN` (e.g. `1979-03`, `2017-02`) |
| `title` | opinion title (first sentence of the PDF SYNOPSIS) |
| `text` | full opinion text (born-digital PDF, no OCR) |
| `date` | issue date, ISO 8601 (best-effort; falls back to `YYYY-01-01`) |
| `issuer` | Vermont Bar Association — Professional Responsibility Committee |
| `jurisdiction` | `US-VT` |
| `url` | link to the original PDF |

## License

[Public Domain / freely published advisory opinions](https://www.vtbar.org/advisory_ethics/)
— VBA Advisory Ethics Opinions are published free to the public on vtbar.org as
born-digital PDFs, interpreting the Vermont Rules of Professional Conduct. They
are advisory (no weight of law) and carry no login, paywall or terms prohibiting
reuse. Treated as effectively public domain, consistent with the other
state-bar legal-ethics sources (`US/NY-LegalEthics`, `US/IL-LegalEthics`,
`US/CT-LegalEthics`). Note: the VBA is a **voluntary** bar association (not an
integrated/state bar), so the 17 U.S.C. § 105 government-edicts rationale is
weaker than for court-arm boards; flagged here as freely-published advisory
material with **commercial use permitted**.
