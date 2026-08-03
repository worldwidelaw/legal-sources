# US/IA-EthicsOpinions — Iowa Ethics and Campaign Disclosure Board — Advisory Opinions

Full-text advisory opinions of the **Iowa Ethics and Campaign Disclosure Board
(IECDB)**. Each opinion is the Board's written interpretation of **Iowa Code
ch. 68A** (campaign finance) and **ch. 68B** (government ethics, lobbying, gifts,
conflicts of interest) and the Board's rules, requested by an official,
candidate, committee, lobbyist or agency (or issued sua sponte) = **doctrine**.

- **Publisher:** Iowa Ethics and Campaign Disclosure Board
- **Coverage:** ~259 advisory opinions
- **Type:** doctrine
- **Jurisdiction:** US-IA

## Access

No JavaScript, CAPTCHA, or authentication. A paginated listing —
`https://ethics.iowa.gov/rulings/advisory-opinions?page={p}` (p = 0, 1, 2, …) —
links each opinion's born-digital HTML detail page at
`/rulings/advisory-opinions/iecdb-ao-{YYYY}-{NN}`. The listing anchor carries the
opinion number and subject; the detail page holds the full text in
`<div class="node__content">`, with the issue date as the first
`Month DD, YYYY` in the body.

## Usage

```bash
python bootstrap.py bootstrap            # Full pull (all opinions)
python bootstrap.py bootstrap --sample   # ~12 samples (newest first)
python bootstrap.py test-api             # Connectivity + extraction test
```

## License

[Public Domain — Iowa state government edict / public record](https://www.law.cornell.edu/uscode/text/17/105) — IECDB advisory opinions are official public records of an Iowa state agency interpreting statute (government-edict works); no attribution required, commercial use permitted.
