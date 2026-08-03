# US/PA-Code — The Pennsylvania Code (State Administrative Regulations)

The **Pennsylvania Code** is the official codification of the rules and
regulations of the Commonwealth of Pennsylvania's executive-branch agencies,
adopted under the Commonwealth Documents Law and published by the Legislative
Reference Bureau. It is the state analogue of the federal CFR, organized by
numbered **Titles → Parts → Chapters → Sections**. Each codified Section is a
regulation → `legislation`.

## Source

- **Official site:** https://www.pacodeandbulletin.gov/Home/Pacode
- **Type:** static HTML full-text database (no CAPTCHA, no JS, no auth)
- **Coverage:** all ~40 in-force Titles of the Pennsylvania Code

## Build recipe

1. Fetch `/Home/Pacode` — the list of Titles is embedded as
   `<option value="/{NNN}/{NNN}toc.html">` tags.
2. For each Title, fetch the Title TOC
   `/secure/pacode/data/{NNN}/{NNN}toc.html` and extract the per-Chapter TOC
   links (`chapter{N}/chap{N}toc.html`).
3. For each Chapter TOC, extract the Section HTML links (`sX.Y.html`).
4. For each Section HTML page, extract the born-digital full text and the
   `<meta>` tags (`title2`, `chapter2`, `section2`) for stable ids/citations.
   The latest effective date is parsed from the "Source" note
   (`effective Month D, YYYY`) when present, else `null`.

```bash
python bootstrap.py test-api            # connectivity + extraction check
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull (streams to data/records.jsonl)
```

## Record schema

`_id`, `_source`, `_type` (`legislation`), `_fetched_at`, `record_id`
(`{title}-{section}`), `citation` (`34 Pa. Code § 31.1`), `title_number`,
`title_name`, `chapter`, `section`, `issuer`, `title`, `text` (full section
body), `date` (ISO 8601 or null), `url`, `jurisdiction` (`US-PA`).

## License

Public Domain — official works of Pennsylvania state government (edicts of a
government agency) are not subject to copyright under the government-edicts
doctrine.

[Public Domain — 17 U.S.C. § 105 / government edicts](https://www.law.cornell.edu/uscode/text/17/105) — no attribution required.

> Note: the publisher's page carries a notice that the compilation text may not
> be "reproduced for profit or sold for profit." This is a non-copyright
> republishing notice on the compiled database, not a restriction on the
> underlying public-domain legal text. Commercial use of the law itself is
> permitted.
