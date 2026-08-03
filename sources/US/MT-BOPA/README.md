# US/MT-BOPA — Montana Board of Personnel Appeals — Decisions

Full-text decisions of the **Montana Board of Personnel Appeals (BOPA)**, the
independent tribunal within the Montana Department of Labor & Industry
(Employment Standards Division) that adjudicates public-sector
collective-bargaining disputes for the State of Montana, its political
subdivisions, and school districts under the **Montana Public Employees
Collective Bargaining Act** (Title 39, Ch. 31, MCA).

BOPA resolves:

- **Interest-arbitration impasses** — including firefighter and police interest
  arbitration awards (§ 39-34-101 et seq., MCA)
- **Fact-finding recommendations** in collective-bargaining impasses
- **Employee-classification appeals**

Each award / decision resolves a specific contested case → `case_law`.

## Data source & method

The BOPA "Case Decision Index" at `erd.dli.mt.gov` publishes its born-digital
decision PDFs across four HTML category pages under:

```
/labor-standards/collective-bargaining/board-of-personnel-appeals/
  case-decision-index/{category}
```

where `{category}` is one of `Employee-Classification-Decisions`,
`fact-finder-decisions`, `firefighter-factfinding`,
`police-interest-arbitration-awards`.

The scraper fetches the four category pages, extracts and de-duplicates the
decision PDF links (filtering to host `erd.dli.mt.gov`, which drops a stray
non-decision IT-policy link on `mt.gov`), downloads each PDF, extracts full text
with `common.pdf_extract` (born-digital text layer; OCR fallback for a couple of
image-only PDFs), and parses the case number and decision date from the
filename / body. No auth, no CAPTCHA.

### Scope note

The **bulk board decisions** (unfair-labor-practice and unit-determination
orders — the legacy `cbdecNNNN` PDFs) are only reachable through the
`ebizws.mt.gov` `ERD_PUBLICPORTAL` search, which sits behind an F5/Shape (TSPD)
bot-challenge (`Request Rejected`) and cannot be enumerated without a browser;
the legacy `dli.mt.gov/hearings/decisions` tree serves individual PDFs but its
directory index returns 403 and it has no Wayback preservation. Those are a
VPS/browser-only future extension. The manifest `jurisdictions` scope is
therefore `partial`.

## Usage

```bash
# Full pull (run with a Python that has PyMuPDF + tesseract on PATH for OCR)
PATH=/opt/homebrew/bin:$PATH /usr/bin/python3 bootstrap.py bootstrap

# Sample (~15 records)
PATH=/opt/homebrew/bin:$PATH /usr/bin/python3 bootstrap.py bootstrap --sample

# Connectivity / extraction test
/usr/bin/python3 bootstrap.py test-api
```

## Record schema

| field | description |
|-------|-------------|
| `_id` | `US/MT-BOPA/{filename-slug}` |
| `_source` | `US/MT-BOPA` |
| `_type` | `case_law` |
| `record_id` | slug derived from the decision PDF filename |
| `case_number` | BOPA case number (e.g. `2025DRS00184`); null if absent |
| `category` | interest arbitration / fact finding / classification |
| `issuer` | Montana Board of Personnel Appeals (BOPA) |
| `title` | Montana BOPA — parties / subject |
| `text` | **full decision text** (PDF extract) |
| `date` | decision date (ISO 8601), from filename/body; null if absent |
| `url` | original decision PDF URL |
| `jurisdiction` | `US-MT` |

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) — decisions of the Montana Board of Personnel Appeals are official works of Montana state government (edicts of a government agency) and are not subject to copyright under the government-edicts doctrine. Free to use, including commercially. No attribution required.
