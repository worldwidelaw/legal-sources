# US/WI-WERC — Wisconsin Employment Relations Commission (Decisions)

Full text of the labor-relations decisions of the **Wisconsin Employment
Relations Commission (WERC)**, the state's quasi-judicial agency administering
Wisconsin's labor-relations statutes — the **Municipal Employment Relations Act
(MERA)**, the **State Employment Labor Relations Act (SELRA)**, and the
**Wisconsin Employment Peace Act (WEPA)**.

The Commission decides representation and election petitions, unit
clarifications, prohibited-practice (unfair-labor-practice) complaints,
declaratory rulings, and related contested cases. Each decision resolves a
specific case and is `case_law`.

- **Coverage:** ~2,835 decisions, July 1989 – present.
- **Jurisdiction:** US-WI
- **Type:** `case_law`

## Source & method

Decisions are indexed by date range on a set of HTML tables linked from
<https://werc.wi.gov/labor-relations-decisions-2/>:

```
/DOAroot/decisions_july-89_dec-98.htm
/DOAroot/decisions_1999.htm
/DOAroot/decisions_2000.htm
/DOAroot/decisions_jan-01_oct-03.htm
/DOAroot/decisions_pdf_nov_2003-dec_2004.htm
/DOAroot/decisions_pdf_2005-2007.htm
/DOAroot/decisions_pdf_2008-2010.htm
/DOAroot/decisions_pdf_2011-2013.htm
/DOAroot/decisions_pdf_2014_on.htm
```

Each index page is an HTML table, one row per decision, with columns *Date
Issued | Decision Docket Identification | Decision Type | Decision Author | PDF
Filename*. The filename (older pages hyperlink it, e.g. `29094-A`; modern pages
show plain text `41400-A.pdf`) names a born-digital decision PDF served at
`https://werc.wi.gov/decisions/{filename}`. The scraper parses every index
table (dates in `MM/DD/YYYY` on modern pages, `MM-DD-YY` on older ones),
downloads each PDF, and extracts full text with the shared `common.pdf_extract`
extractor (older scanned decisions go through OCR).

Note: the bare `/decisions/` directory listing returns HTTP 403, but individual
PDF files under it serve 200; the index pages are reached via the
`labor-relations-decisions-2` landing page.

## Usage

```bash
python bootstrap.py test-api            # Connectivity + extraction test
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # Full pull
python bootstrap.py bootstrap-fast      # Alias for full pull (VPS wrapper)
```

## License

[Public Domain — U.S. state government edict](https://www.law.cornell.edu/uscode/text/17/105) — decisions of the Wisconsin Employment Relations Commission are official works of Wisconsin state government (edicts of a quasi-judicial government body) and are not subject to copyright under the government-edicts doctrine. Free to use, including commercially.
