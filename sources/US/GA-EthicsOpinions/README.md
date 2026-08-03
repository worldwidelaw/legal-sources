# US/GA-EthicsOpinions — Georgia Government Transparency and Campaign Finance Commission — Advisory Opinions

Formal **advisory opinions** issued by the Georgia Government Transparency and
Campaign Finance Commission (formerly the Georgia State Ethics Commission)
interpreting the Georgia Government Transparency and Campaign Finance Act
(O.C.G.A. Title 21, ch. 5) — campaign finance, financial disclosure, lobbyist
registration and conflicts of interest — and the Commission's rules. An
advisory opinion is the Commission's written interpretation of those statutes,
requested by a candidate, public officer, committee, lobbyist or agency =
**doctrine**. Corpus spans **1987–present** (~90 opinions).

## Access

The Commission's site (`ethics.ga.gov`) is WordPress. The **Advisory Opinions**
category is id **23**. A single WP REST API call enumerates the full corpus and
returns full text inline:

```
GET https://ethics.ga.gov/wp-json/wp/v2/posts?categories=23&per_page=100
```

Each post's `content.rendered` carries the born-digital opinion text (the lead
"PDF Copy of the Advisory Opinion" link line is stripped). Opinion number is
parsed from `title.rendered` ("Advisory Opinion: 2023-01"); records are deduped
by number keeping the longest body (a few posts are duplicated re-publishes).
No JavaScript, no CAPTCHA, no auth. Full text via HTML strip, **no OCR**.

## Usage

```bash
python bootstrap.py test-api             # Connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 samples (newest first)
python bootstrap.py bootstrap            # Full pull (all opinions)
```

## Record shape

`_id`, `_source`, `_type` (doctrine), `_fetched_at`, `opinion_number`, `issuer`,
`title`, `text` (full opinion), `date` (ISO 8601), `url`, `jurisdiction`
(US-GA).

## License

[Public Domain — Georgia state government edict / public record](https://www.law.cornell.edu/uscode/text/17/105) — advisory opinions of the Georgia Government Transparency and Campaign Finance Commission are official public records of a Georgia state agency interpreting statute (government-edict works, published for public use). No attribution required; commercial use permitted.
