# US/NY-LegalEthics — NYSBA Committee on Professional Ethics — Ethics Opinions

Full text of the **Ethics Opinions** issued by the New York State Bar
Association's **Committee on Professional Ethics**. Each opinion is advisory and
expresses the Committee's interpretation of the **New York Rules of Professional
Conduct** (and, for older opinions, the Code of Professional Responsibility) in
response to an attorney's inquiry about their own proposed conduct. This makes
each opinion **doctrine** (the committee's official written interpretation of the
attorney-conduct rules).

The opinions form one continuous numbered series retrospective to 1957 (Opinion
#9 to Opinion #1295+ as of July 2026, ~1,286 opinions), published free to the
public on nysba.org.

This is the **state bar's** attorney-ethics series and is distinct from
`US/NY-EthicsOpinions` (COELIG/JCOPE — political ethics of public officials) and
from New York Attorney General opinions.

## Access

No JavaScript, CAPTCHA, or authentication is required (browser User-Agent).

1. **Discovery** — the *Ethics Opinions* category index paginates every opinion
   as a post at
   [`/category/ethics-opinions/page/{p}/`](https://nysba.org/category/ethics-opinions/)
   (~129 pages, 10 posts each). Each opinion post uses **one of two** permalink
   schemes: `/opinion-{N}/` (older) or `/ethics-opinion-{N}[-slug]/` (newer). The
   scraper reads each `<article>`'s title link and takes the exact href — the
   number is parsed from the href, never constructed (a bare `/ethics-opinion-1/`
   wrongly prefix-redirects to `/opinion-100/`). The walk stops after two
   consecutive empty pages.
2. **Full text** — each opinion page is born-digital HTML; the
   `.single-post-content` body carries the Topic / Digest / Code / Question /
   Opinion text. No OCR. A leading "News Center" / duplicate-title / "View and
   download as PDF" boilerplate is stripped.
3. **Date** — read from the `<meta property="article:published_time">` tag
   (backfilled to the real issue date for older opinions).

## Usage

```bash
python bootstrap.py test-api             # connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # full pull (all opinions)
```

## Output fields

| Field | Description |
|-------|-------------|
| `_id` | `US/NY-LegalEthics/{opinion_number}` |
| `opinion_number` | Opinion number in the continuous NYSBA series (e.g. `1286`, `706`) |
| `title` | Opinion title (page h1) |
| `text` | Full opinion body (Topic / Digest / Code / Question / Opinion) |
| `date` | ISO 8601 `YYYY-MM-DD` from the `article:published_time` meta tag |
| `issuer` | NYSBA — Committee on Professional Ethics |
| `url` | Opinion page URL |

## License

[Public Domain — NYSBA Ethics Opinions](https://nysba.org/committees/committee-on-professional-ethics/) — NYSBA Ethics Opinions are published free to all attorneys and to the public on nysba.org (retrospective to 1957) as an educational service interpreting the New York Rules of Professional Conduct. They are advisory and carry no copyright restriction or terms prohibiting reuse. Commercial use permitted.
