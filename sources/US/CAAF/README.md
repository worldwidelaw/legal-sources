# US/CAAF — U.S. Court of Appeals for the Armed Forces (Opinions)

Full-text precedential opinions of the **United States Court of Appeals for the
Armed Forces (CAAF)**, the Article I civilian appellate court that sits atop the
military-justice system. CAAF exercises worldwide appellate jurisdiction over
active-duty servicemembers and reviews the decisions of the four service Courts
of Criminal Appeals (Army, Navy-Marine Corps, Air Force, Coast Guard). Each
opinion decides a specific court-martial appeal, so the corpus is `case_law`.

## Source

- Website: https://www.armfor.uscourts.gov/newcaaf/opinions.htm
- Access: plain HTTP GET with a browser User-Agent. No auth, no CAPTCHA.

## How it works

1. `GET /newcaaf/opinions.htm` lists ~30 *term of court* index pages
   (`/opinions/{term}.htm`, e.g. `2023OctTerm`, `CurrentOpins`).
2. Each term page is an HTML table with columns
   **CASE NAME | DOCKET # | OPINION DATE | MJ CITATION**; the docket cell links
   the opinion PDF at `/opinions/{term}/{docket}.pdf` (the filename is the docket
   digits, e.g. `240093.pdf` == docket `24-0093`).
3. `GET /opinions/{term}/{docket}.pdf` returns the born-digital opinion PDF.

Case name, docket number, decision date, and Military Justice Reporter citation
are parsed from the per-term index table; the full text is extracted from the
PDF via the shared `common.pdf_extract` helper.

Digitised coverage runs from roughly the 2001 term to the present
(~40–60 opinions per term, ~1,000+ total). Pre-2001 term pages carry no PDF
links and yield nothing.

## Fields

`_id`, `_source`, `_type` (`case_law`), `_fetched_at`, `slug`, `docket_number`,
`citation`, `term`, `court`, `title`, `text` (full opinion), `url`, `date`,
`jurisdiction`.

## Run

```bash
python bootstrap.py test-api           # connectivity + extraction check
python bootstrap.py bootstrap --sample # ~12 sample records
python bootstrap.py bootstrap          # full pull
```

Requires `bs4` and a PDF backend for `common.pdf_extract` (locally: run with the
interpreter that has both, e.g. `/usr/bin/python3`).

## License

[Public Domain (U.S. Government Work — 17 U.S.C. § 105)](https://www.law.cornell.edu/uscode/text/17/105) — opinions of the U.S. Court of Appeals for the Armed Forces are works of the U.S. federal government and are in the public domain (government edicts). Commercial use permitted, no attribution required.
