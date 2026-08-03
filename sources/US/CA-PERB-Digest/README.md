# US/CA-PERB-Digest — California PERB Decisional-Law Digest (Headnotes)

The official **decisional-law digest** of the **California Public Employment
Relations Board (PERB)**. PERB maintains an official annotated digest of its
own case law: each Board Decision is broken into one or more **headnotes**, and
every headnote states a discrete legal principle established by that decision,
classified under a hierarchical topic code (e.g. `1000.02163 – Work Rules`,
under `1000.00000 – SCOPE OF REPRESENTATION`). This state-authored digest of
public-sector labor-law principles = **doctrine**.

Sibling of **US/CA-PERB** (Board Decisions) and **US/CA-PERB-FactFinding**
(fact-finding reports) — the digest is the official topical index/annotation of
PERB precedent.

## Access

`perb.ca.gov` is a WordPress site. Headnotes are the custom post type
`decision-headnote`, enumerable via the public WP REST API (no CAPTCHA, no
auth):

```
GET /wp-json/wp/v2/decision-headnote?per_page=100&page={N}   # ~15,019 posts
```

The WP title encodes the decision number, topic code and topic name
(`Headnote for 3017E, 1000.02163 – Work Rules`). The headnote's substantive
text is server-rendered on its page (the ACF fields are empty over REST), so
each headnote page is fetched and the topic classification (`<strong>`
`NNNN.NNN` lines) plus the holding paragraph are extracted from the
`<article>`. The two navigation paragraphs ("View all topics …", "Full Decision
Text …") carry `<a>` links and are excluded.

## Usage

```bash
python bootstrap.py test-api              # connectivity + extraction check
python bootstrap.py bootstrap --sample    # ~12 sample records
python bootstrap.py bootstrap             # full pull (~15,019 headnotes)
```

## Data

- `_type`: `doctrine`
- ~15,019 headnotes
- Fields: `decision_number`, `topic_code`, `topic_name`, `classification`,
  `decision_caption`, `title`, `text` (classification + holding statement),
  `date`, `url`, `jurisdiction` (US-CA)

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) —
the decisional-law digest of the California Public Employment Relations Board is
an official state-government work in the public domain under the
government-edicts doctrine. Commercial use permitted; no attribution required.
