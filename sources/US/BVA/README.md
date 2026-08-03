# US/BVA — Board of Veterans' Appeals: Decisions

Full text of the decisions of the **Board of Veterans' Appeals (BVA)**, the
appellate body within the U.S. Department of Veterans Affairs that issues the
final agency decision on appeals of VA benefit claims (service connection,
disability ratings, effective dates, dependency and indemnity compensation,
etc.). Each decision adjudicates a specific veteran's contested appeal, so
these are **case_law**.

## Source

- Sitemap index: https://www.va.gov/sitemap_bva.xml
- Per-year sitemaps: `https://www.va.gov/vetapp{YY}/sitemap.xml` (1992–present)
- Decision files: `https://www.va.gov/vetapp{YY}/Files{N}/{CITATION}.txt`

The Board has published every decision as a plain-text file since ~1992. The
public sitemap index enumerates ~35 per-year sitemaps, each listing tens of
thousands of decision URLs (**~1.5M+ decisions total**). Each `.txt` is
Windows-1252 plain text with a structured header:

```
Citation Nr: A25087406
Decision Date: 10/09/25   Archive Date: 10/09/25
DOCKET NO. 250226-520147
DATE: October 9, 2025
ORDER ... FINDING OF FACT ... CONCLUSIONS OF LAW ... REASONS AND BASES ...
```

## How it works

1. Fetch `/sitemap_bva.xml` → per-year sitemap URLs.
2. Stream each year's sitemap → decision `.txt` URLs.
3. Fetch each `.txt` (curl + Chrome UA), decode cp1252 → full text.
4. Parse citation number, docket number and decision date from the header.

No PDF extraction, no OCR, no JavaScript, no CAPTCHA, no auth. va.gov returns
HTTP 200 only for a browser User-Agent, so every URL is fetched via curl with
a Chrome UA. Older years list lowercase `/files{N}/` paths with an `http://`
scheme; the URL is used as-listed and normalised to `https`.

## Usage

```bash
python bootstrap.py test-api             # Connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample decisions
python bootstrap.py bootstrap            # Full pull (~1.5M decisions)
python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
```

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) — decisions of the Board of Veterans' Appeals are works of the U.S. federal government and carry no copyright. Commercial use permitted; no attribution required.
