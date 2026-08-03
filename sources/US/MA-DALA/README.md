# US/MA-DALA — Massachusetts Division of Administrative Law Appeals (General Jurisdiction Decisions)

Full text of the **General Jurisdiction decisions** published by the
Massachusetts **Division of Administrative Law Appeals (DALA)** — the
Commonwealth's central, independent administrative tribunal. DALA
magistrates hear contested "adjudicatory proceedings" between a person
and a Massachusetts state agency and issue a written decision resolving
each specific case = **case_law**.

By volume the corpus is dominated by **contributory-retirement appeals**
(`CR-*`, appeals from the state and local public-employee retirement
boards), followed by Fair Labor Division citations (`LB-*`), Department
of Environmental Protection matters (`DEP-*`), Veterans' Services
(`VS-*`), Public Health (`PH`/`PHET-*`), Disabled Persons Protection
Commission (`DPPC-*`), Board of Registration in Medicine (`RM-*`), and
others.

## Source

- Landing: https://www.mass.gov/general-jurisdiction-decisions
- Listing (2016–present): https://www.mass.gov/lists/general-jurisdiction-decisions-2016-to-present (~592 documents)
- Listing (through 2010): https://www.mass.gov/lists/general-jurisdiction-decisions-through-2010 (~27 documents)
- Document URL: `https://www.mass.gov/doc/{slug}/download`

## How it works

1. GET each of the two Mass.gov listing pages (server-rendered HTML).
2. Collect every `/doc/{slug}/download` link. The `{slug}` encodes the
   party caption and the DALA docket (`{CATEGORY}-{YY}-{NNNN}`), from
   which the scraper parses `case_number`, `category`, `parties`, and
   `year` **without** touching the document.
3. Download each PDF (curl, browser UA, ~1 req/s) and extract its text
   layer via `common.pdf_extract`. A `<200`-char guard skips image-only
   scans; an Akamai "This page is forbidden" sentinel guard skips 403
   responses served as fake PDFs.

## Vantage note (why this is `planned`, not `complete`)

The Mass.gov document CDN is **Akamai bot-managed**. The listing pages
load cleanly from any vantage — so discovery and metadata (party
caption, docket number, category, year) are fully verified locally (597
documents, 544 with a parseable docket). But `/doc/*/download` returns a
403 "This page is forbidden" page (rendered as a 2-page PDF) to
datacenter / bursty clients, and after a few requests Akamai also 403s
the listing pages for that IP. The decision **bodies** therefore could
not be validated from the build vantage. Launch from a residential /
less-throttled vantage (or browser automation) that can pull
`/doc/*/download`, confirm full text, then mark the source `complete`.

## Commands

```bash
python bootstrap.py test-api            # discovery + extraction smoke test
python bootstrap.py bootstrap --sample  # ~12 sample decisions
python bootstrap.py bootstrap           # full pull
python bootstrap.py bootstrap-fast      # alias for full pull (VPS wrapper)
```

## License

[Public Domain — 17 U.S.C. § 105 (US government-edicts doctrine)](https://www.law.cornell.edu/uscode/text/17/105) — decisions of the Massachusetts Division of Administrative Law Appeals are official state-government works in the public domain. Commercial use permitted; no attribution required.
