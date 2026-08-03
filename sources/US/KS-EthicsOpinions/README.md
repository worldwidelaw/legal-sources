# US/KS-EthicsOpinions — Kansas Governmental Ethics Commission Advisory Opinions

Advisory opinions issued by the **Kansas Governmental Ethics Commission**
(formerly the Kansas Commission on Governmental Standards & Conduct;
administered via the Kansas Public Disclosure Commission). Under
**K.S.A. 46-254** the Commission issues advisory opinions construing the Kansas
governmental-ethics statutes — the state conflict-of-interest provisions
(K.S.A. 46-215 et seq.), the campaign-finance act (K.S.A. 25-4142 et seq.) and
the lobbying-disclosure statutes. Each opinion is a public record filed with the
Secretary of State and published on kansas.gov. Because each opinion is the
Commission's official written interpretation of the ethics statutes, the corpus
is classified as **doctrine**.

## Source

- **Publisher:** Kansas Governmental Ethics Commission / Kansas Public Disclosure Commission
- **URL:** https://www.kansas.gov/kpdc-opinion/
- **Opinion pages:** https://www.kansas.gov/kpdc-opinion/opinion/view/{id}
- **Access:** public HTTP, no auth, no CAPTCHA, no JavaScript required
- **Format:** born-digital **HTML** (real text, no PDF, no OCR)
- **Coverage:** ~1,800 advisory opinions, 1990–present (the digitized set)

## How it works

Each opinion is a born-digital HTML page whose `<main id="main-content">`
container holds the entire opinion body: the issue date, `Opinion No. YYYY-NN`,
the recipient, a **Synopsis**, the **Cited herein** statutes and the full
opinion letter. The scraper extracts that container's text directly.

**Enumeration.** The site's three search endpoints each cap at 10 results
server-side and ignore paging, so they expose only ~40 recent opinions. The full
historical corpus is reached by a bounded `/opinion/view/{id}` scan: the ids form
a near-contiguous block from ~660 (Opinion 1990-04) to ~2496 (Opinion 2024-03).
A "miss" page renders the search form (no `Opinion No.`); a "hit" page contains
`Opinion No. YYYY-NN`. `fetch_all` yields the ~40 recent search-area opinions
first (they span years, so they make representative `--sample` output), then
scans the id range to fill in the historical corpus, deduped by opinion number.

- **Date** is parsed from the first `Month DD, YYYY` in the opinion body, with a
  fallback to the year embedded in the opinion number.

## Usage

```bash
python bootstrap.py bootstrap            # Full pull (all advisory opinions)
python bootstrap.py bootstrap --sample   # Fetch ~12 samples
python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
python bootstrap.py test-api             # Connectivity + extraction test
```

## Record schema

| field | description |
|-------|-------------|
| `_id` | `US/KS-EthicsOpinions/{opinion_number}` |
| `_source` | `US/KS-EthicsOpinions` |
| `_type` | `doctrine` |
| `opinion_number` | e.g. `2024-01`, `1997-44` |
| `title` | opinion number + synopsis caption |
| `text` | full opinion body (clean HTML text) |
| `date` | ISO 8601 issue date |
| `url` | link to the original opinion page |
| `jurisdiction` | `US-KS` |

## License

[Public Domain (State of Kansas Government Work)](https://www.law.cornell.edu/uscode/text/17/105) — advisory opinions of the Kansas Governmental Ethics Commission are official public records of the State of Kansas, filed with the Secretary of State and published for public use with no copyright restriction. Commercial use permitted.
