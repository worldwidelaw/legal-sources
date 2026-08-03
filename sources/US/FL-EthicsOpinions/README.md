# US/FL-EthicsOpinions — Florida Commission on Ethics — Advisory Opinions (CEO)

Full text of the formal advisory opinions ("**CEO**" opinions) of the Florida
Commission on Ethics, issued under Part III, ch. 112, Fla. Stat. (the Code of
Ethics for Public Officers and Employees) and § 112.322(3).

Each opinion is the Commission's written, authoritative interpretation of
Florida's conflict-of-interest, voting-conflict, gift, honoraria,
financial-disclosure and standards-of-conduct laws, applied to the facts a
public officer, employee or candidate presents in a request; the requestor may
rely on the opinion. These are official state legal interpretations =
**doctrine**.

## Access

- **No** JavaScript, CAPTCHA, or authentication.
- Opinions are born-digital, full-text HTML pages at
  `https://www.ethics.state.fl.us/Documents/Opinions/{YY}/CEO {YY}-{NNN}.htm`
  (the filename contains a literal space; percent-encoded on fetch).
- The index `/Research/Opinions.aspx` links a per-year list page
  (`/Research/OpinionsLists/List{YY}.aspx`) for every year the Commission has
  issued opinions (List74–List99 for 1974–1999, List00–present for 2000–now).
  The scraper reads the index, then each year list, collects the opinion links
  (~2,500+ opinions over ~50 years), fetches each, strips the HTML to clean
  text, and parses the CEO number (filename) and issue date (body).

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
(`US-FL`).

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105)
— Advisory opinions of the Florida Commission on Ethics are official
state-government works in the public domain under the government-edicts
doctrine. Commercial use permitted; no attribution required.
