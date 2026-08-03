# US/NY-AdvisoryOpinions — New York State Advisory Opinions (TSB-A)

Full text of the **Advisory Opinions (TSB-A)** issued by the New York State
Department of Taxation and Finance, Office of Counsel. An Advisory Opinion is
the Department's official written interpretation of how New York tax law
applies to the specific facts presented by a petitioner (Tax Law § 171,
Twenty-fourth; 20 NYCRR Part 2376). It is binding on the Department only with
respect to that petitioner and only on the facts described, so the corpus is
the Department's **interpretive guidance → `doctrine`**.

These are distinct from the adjudicatory decisions of the NYS Division of Tax
Appeals / Tax Appeals Tribunal, which are `case_law` and covered by
**US/NY-TaxAppeals**.

The opinions span every New York tax type — income, sales & use, corporation,
estate & gift, alcoholic beverage, cigarette, fuel/petroleum, mortgage
recording, real estate transfer, highway use, and more — from 1980 to present.

## Access

No JavaScript, no CAPTCHA, no auth — reachable from a normal client.

`www.tax.ny.gov` serves a server-rendered index tree under
`/pubs_and_bulls/advisory_opinions/`. The hub page `ao_tax_types.htm` links one
index page per tax type (e.g. `income_ao.htm`, `sales_ao.htm`); each tax-type
index links the current opinions plus per-year archive pages back to 1980.

Within the `/advisory_opinions/` tree the URL shape distinguishes index from
opinion:

- 1 path segment → **index** page (e.g. `.../advisory_opinions/income_ao.htm`,
  `income_ao_1980.htm`)
- 2 path segments → **HTML opinion** (e.g. `.../advisory_opinions/income/24-2i.htm`)
- Older opinions are **PDFs** at `/pdf/advisory_opinions/{type}/a{YY}_{N}{t}.pdf`

The scraper BFS-crawls the index tree from `ao_tax_types.htm`, collecting
opinion links (HTML two-segment pages + PDFs). HTML opinions are read from the
`<main id="tax-content">` body; PDF opinions go through `common.pdf_extract`
(born-digital text layer). The TSB-A number, tax type and issue date are
parsed from the document; a `<150`-char guard skips the rare empty/scanned page
(recoverable on an OCR host).

## Usage

```bash
python bootstrap.py bootstrap            # Full pull
python bootstrap.py bootstrap --sample   # Fetch ~12 samples
python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
python bootstrap.py test-api             # Connectivity test
```

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) —
Advisory Opinions of the New York State Department of Taxation and Finance are
official state-government works published as government edicts / interpretive
guidance, in the public domain. No attribution required; commercial use
permitted.
