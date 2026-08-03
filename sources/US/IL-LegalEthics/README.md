# US/IL-LegalEthics — Illinois State Bar Association (ISBA) Professional Conduct Advisory Opinions

Full text of the **Advisory Opinions on Professional Conduct** issued by the
Illinois State Bar Association's Standing Committee on Professional Conduct. Each
opinion expresses the ISBA's interpretation of the **Illinois Rules of
Professional Conduct (IRPC)** in response to a stated hypothetical fact
situation, advising lawyers whether the described conduct is proper. This makes
each opinion **doctrine** (the committee's official written interpretation of the
attorney-conduct rules).

The online archive spans the older sequentially numbered opinions (Opinion No.
6xx/7xx, late 1970s–1983) through the modern `{YY}-{NN}` series (Opinion No.
84-1 to the present), ~389 opinions as of July 2026.

This is the **state bar's** attorney-ethics series and is distinct from
`US/IL-AGOpinions` (Illinois Attorney General opinions).

## Access

No JavaScript, CAPTCHA, or authentication is required (browser User-Agent).

1. **Discovery** — the *Ethics Opinions by Year* index
   [`/ethics/years`](https://www.isba.org/ethics/years) embeds a direct link
   `/ethics/opinions/{id}` for every opinion. The scraper regex-extracts and
   de-duplicates all such links.
2. **Full text** — each opinion page is a born-digital HTML page. The
   `<article>` body carries labelled *Opinion Number* / *Opinion Date* fields,
   the topical `h1` title, and the Digest / Facts / Question / Opinion sections
   plus references. No OCR is needed. The trailing *See Related Opinions*
   navigation is trimmed; referenced IRPC rules are captured into a `rules`
   field.

## Usage

```bash
python bootstrap.py test-api             # connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # full pull (all opinions)
```

## Output fields

| Field | Description |
|-------|-------------|
| `_id` | `US/IL-LegalEthics/{opinion_number}` |
| `opinion_number` | `24-01` (modern) or `700` (older sequential) |
| `title` | Topical opinion title (page h1) |
| `text` | Full opinion body (Digest / Facts / Question / Opinion + references) |
| `date` | ISO 8601 `YYYY-MM-01`, parsed from the *Opinion Date* field |
| `rules` | Referenced IRPC rule numbers |
| `issuer` | ISBA — Standing Committee on Professional Conduct |
| `url` | Opinion page URL |

## License

[Public Domain — ISBA Professional Conduct Advisory Opinions](https://www.isba.org/ethics) — ISBA Advisory Opinions are published free to the public on isba.org as an educational service interpreting the Illinois Rules of Professional Conduct. They are advisory (no weight of law) and carry no copyright restriction or terms prohibiting reuse. Commercial use permitted.
