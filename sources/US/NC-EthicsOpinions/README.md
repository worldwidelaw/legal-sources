# US/NC-EthicsOpinions — North Carolina Ethics Commission Formal Advisory Opinions

Full text of the formal advisory opinions of the **North Carolina Ethics
Commission**, the independent state body that administers the State Government
Ethics Act (N.C.G.S. Chapter 138A) and the Lobbying Law (Chapter 120C).

The Commission publishes three series of formal opinions, all **doctrine**:

- **Formal Ethics Advisory Opinions** — caption `E-YY-NNN`
- **Formal Legislative Advisory Opinions** — caption `L-YY-NNN`
- **Formal Lobbying Advisory Opinions** — caption `LB-YY-NNN`

Each opinion is the Commission's authoritative written interpretation of the
State's ethics/lobbying statutes, adopted by the Commission and published with
the requester's identifying information edited out.

## Access

`ethics.nc.gov` is a Drupal site. Each series has a listing page
`/advisory-opinions/{series-slug}` whose rows are anchors whose text is the
opinion number (`E-15-004`) and whose `href` points at the born-digital opinion
PDF — either a media entity `/media/{id}/open` or `/aos/{slug}/download?attachment`.
The scraper parses each series page, dedups by opinion number, downloads each PDF
and extracts full text via the shared `common.pdf_extract` backend chain. No
JavaScript, CAPTCHA or authentication is required (a browser User-Agent is used).

~80 distinct formal opinions across the three series.

## Usage

```bash
python bootstrap.py bootstrap            # Full pull (all opinions)
python bootstrap.py bootstrap --sample   # Fetch ~12 samples
python bootstrap.py test-api             # Connectivity + extraction test
```

## Output schema

`_id`, `_source`, `_type` (doctrine), `_fetched_at`, `opinion_number`,
`document_type`, `issuer`, `title`, `text` (full text), `url`, `date`,
`jurisdiction` (US-NC).

## License

[Public Domain — North Carolina state government edict / public record](https://www.law.cornell.edu/uscode/text/17/105) — formal advisory opinions of the North Carolina Ethics Commission are official public records of a North Carolina state agency interpreting statute (government-edict works), adopted by the Commission and published for public use under the State Government Ethics Act (N.C.G.S. Ch. 138A). No attribution required; commercial use permitted.
