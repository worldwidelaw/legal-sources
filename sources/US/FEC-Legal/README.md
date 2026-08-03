# US/FEC-Legal — Federal Election Commission: Advisory Opinions & Enforcement Matters (MURs)

Full-text corpus of the U.S. Federal Election Commission's legal output, via the
official open REST API (`api.open.fec.gov`) plus born-digital PDF documents from `fec.gov`.

## Coverage

| Class | `_type` | Count | Content |
|-------|---------|-------|---------|
| Advisory Opinions (AOs) | `doctrine` | ~970 | Formal interpretive opinions on how federal campaign-finance law applies to a requestor's proposed activity. Full text of the Commission's Final Opinion. |
| Enforcement Matters (MURs, "Matters Under Review") | `case_law` | ~4,800 | Closed enforcement cases: General Counsel's Reports, Factual & Legal Analyses, Commission certifications, conciliation & settlement agreements. |

## Data access

- **Index / search:** `GET https://api.open.fec.gov/v1/legal/search/?type={advisory_opinions|murs}&from_hit=N&hits_returned=20`
  Returns matter metadata plus a `documents[]` array (each with a relative `url`).
- **Documents:** `https://www.fec.gov{document.url}` — born-digital PDFs, extracted with the
  shared `common.pdf_extract` extractor.

For each AO the scraper prefers the **Final Opinion** document; for each MUR it concatenates
the text of the matter's documents (labelled by category).

## API key

The FEC / api.data.gov API requires a key. `DEMO_KEY` works out of the box but is
rate-limited (~30 requests/hour) — enough for sampling, not for a full run. For the full
corpus set `FEC_API_KEY` to a **free, instant** key from <https://api.data.gov/signup/>.

## Usage

```bash
python bootstrap.py test                # connectivity + one AO full-text check
python bootstrap.py bootstrap --sample  # 10+ sample records to sample/
FEC_API_KEY=... python bootstrap.py bootstrap   # full run -> data/records.jsonl
```

## License

[Public Domain — U.S. Government Work](https://www.law.cornell.edu/uscode/text/17/105) — works of the U.S. federal government are not subject to copyright (17 U.S.C. § 105). Commercial use permitted, no attribution required.
