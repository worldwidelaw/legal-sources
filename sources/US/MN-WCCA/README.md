# US/MN-WCCA — Minnesota Workers' Compensation Court of Appeals

Published decisions of the **Minnesota Workers' Compensation Court of Appeals**
(WCCA), a standing appellate court of record created by Minn. Stat. § 175A. The
WCCA reviews the decisions of the state's compensation judges; its own decisions
are reviewable only by the Minnesota Supreme Court on certiorari. The corpus also
includes the Supreme Court's workers' compensation opinions, which the court
publishes alongside its own in a `sup/` directory.

- **Type:** `case_law`
- **Coverage:** ~3,600 unique decisions, 1988–present
- **Site:** https://mn.gov/workcomp/
- **Documents:** https://mn.gov/workcomp-stat/

## How it works

Enumeration uses the server's own recursive directory index,
`https://mn.gov/workcomp-stat/.doclist.html`, which lists every published file
(~4,250: one directory per year plus `sup/`). No search API, pagination or
JavaScript is involved.

For 1988–2001 the same decision is often published twice — as HTML and as a
redacted PDF. Same-stem duplicates collapse to a single record, preferring the
HTML; PDF-only decisions (mostly 1988–1996) are extracted with the shared
`common/pdf_extract` helper.

Opinions from before roughly 2007 are WordPerfect exports: their typographic
punctuation is stored as ASCII inside
`<span style='font-family:"WP TypographicSymbols"'>`, so an apostrophe arrives as
`=` and a section sign as `'`. The scraper maps those glyphs back
(`=`→`’`, `A`→`“`, `@`→`”`, `'`→`§`) before stripping tags, otherwise the text
reads `the employee = s claim ... Minn. Stat. ' 176.141`.

## Access note — Radware Bot Manager

`mn.gov` sits behind Radware Bot Manager. A plain `requests` User-Agent is
answered with a `validate.perfdrive.com` JS interstitial (HTTP 200, ~21 KB, no
decision content). A **complete browser header set** — UA plus `Accept`,
`Accept-Language`, `sec-ch-ua*`, `Sec-Fetch-*` and `Upgrade-Insecure-Requests` —
is served the real document, and that is what the scraper always sends.

Radware additionally applies a sticky per-egress-IP reputation ban that trips
after a burst of requests. When it does, the scraper falls back to the Internet
Archive (`web.archive.org`), which holds only ~420 of the ~3,600 decisions, and
logs a loud warning. The archive is a degraded mode, **not** a substitute for an
unblocked vantage: run the full bootstrap from a US residential vantage, pace it
at ~1 req/s, and check the log for the fallback warning before trusting a count.

Record `url` always points at the canonical `mn.gov` address, whichever transport
delivered the bytes.

## Usage

```bash
python bootstrap.py test-api          # index reachable + one document has full text
python bootstrap.py bootstrap --sample
python bootstrap.py bootstrap         # sequential full pull
python bootstrap.py bootstrap-fast    # concurrent full pull (VPS wrapper)
```

## Record shape

| Field | Notes |
|---|---|
| `_id` | `MN-WCCA-{year}-{slugified filename}` |
| `title` | Short case name from `<title>` |
| `text` | Full opinion text (samples: 6.4K–30K chars) |
| `date` | Decision date from the `date` meta tag, else the filename |
| `court` | `Minnesota Workers' Compensation Court of Appeals` or `Minnesota Supreme Court` |
| `docket_number` | `WC19-6311` (WCCA) or `A19-0806` (Supreme Court), when stated |
| `parties` | Full case caption |
| `summary` / `headnote` | Official headnote line from the `description` meta tag |
| `source_format` | `html` or `pdf` |

## License

[Public domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) —
decisions of a US state court are government edicts and are not subject to
copyright. Commercial use is permitted; no attribution required.
