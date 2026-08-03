# US/TX-EthicsOpinions — Texas Ethics Commission — Ethics Advisory Opinions

Full text of the formal **Ethics Advisory Opinions** of the Texas Ethics
Commission (TEC), issued under Tex. Gov't Code ch. 571.

Each opinion is the Commission's written, authoritative interpretation of the
laws it administers — campaign finance and political contributions (Election
Code title 15), personal financial disclosure, lobby registration,
conflict-of-interest and standards-of-conduct statutes, and the bribery/gift
provisions of the Penal Code — applied to the facts presented in a request. A
person who acts in reliance on an advisory opinion has a defense to
prosecution. These are official state legal interpretations = **doctrine**.

## Access

- **No** JavaScript, CAPTCHA, or authentication.
- Opinions are born-digital, full-text HTML pages under
  `https://www.ethics.state.tx.us/opinions/part{I..VIII}/{N}.html`.
- The eight topical **digest pages**
  (`/opinions/part{I..VIII}/digest_{a..h}.php`) are the authoritative index of
  every opinion number and its part directory. The scraper reads all eight,
  collects the unique opinion links (~644 opinions, 1992–present), fetches each,
  strips the HTML to clean text, and parses the opinion number (filename) and
  issue date (body).

## Usage

```bash
python bootstrap.py bootstrap            # Full pull (all advisory opinions)
python bootstrap.py bootstrap --sample   # ~12 samples
python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
python bootstrap.py test-api             # Connectivity + extraction test
```

## Output schema

`_id`, `_source`, `_type` (`doctrine`), `_fetched_at`, `opinion_number`,
`issuer`, `title`, `text` (full opinion body), `url`, `date`, `jurisdiction`
(`US-TX`).

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105)
— Ethics Advisory Opinions of the Texas Ethics Commission are official
state-government works in the public domain under the government-edicts
doctrine. Commercial use permitted; no attribution required.
