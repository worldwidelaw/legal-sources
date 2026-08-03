# US/CIT — U.S. Court of International Trade (Slip Opinions)

Full text of the [U.S. Court of International Trade](https://www.cit.uscourts.gov/)'s
published **slip opinions**. The CIT is a specialized Article III federal court
with exclusive nationwide jurisdiction over civil actions arising out of the
customs and international-trade laws of the United States — antidumping and
countervailing-duty determinations, customs classification and valuation
protests, trade-remedy and enforcement matters, and related cases. Each slip
opinion resolves a specific case, so every record is `case_law`.

## Source

- **Index:** https://www.cit.uscourts.gov/slip-opinions
- **Per-year archives:** `https://www.cit.uscourts.gov/content/slip-opinions-{YYYY}`
  (a couple of years use the shorter `/slip-opinions-{YYYY}` slug — the scraper
  harvests the links from the index page rather than constructing them).
- **Documents:** born-digital PDFs at `https://www.cit.uscourts.gov/sites/cit/files/{YY-NN}.pdf`

## How it works

1. Fetch the slip-opinions index and harvest every per-year archive link
   (2000–present; ~27 years).
2. On each year page, parse the HTML table (columns: Number, Caption, Date,
   Court No., Judge, Jurisdiction). The first cell links the opinion PDF.
3. Download each PDF and extract full text with the shared
   `common.pdf_extract` extractor (born-digital text layer; OCR fallback for
   the rare scan). Records shorter than 300 characters are skipped.

`record_id` is the slip-opinion number `{YY-NN}` (e.g. `25-160`), which is the
natural, unique citation ("Slip Op. 25-160").

Corpus is roughly **3,500–4,000** opinions (≈120–185/year × ~27 years).

## Usage

```bash
python bootstrap.py test-api             # connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # full pull
```

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) —
U.S. Court of International Trade slip opinions are works of a U.S. federal court
(government edicts) and are not subject to copyright. Free to use, including
commercially.
