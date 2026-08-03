# US/MD-LegalEthics — Maryland State Bar Association, Committee on Ethics (Ethics Docket Opinions)

Full text of the **Ethics Docket** opinions issued by the Maryland State Bar
Association (MSBA) **Committee on Ethics**. Each opinion is the Committee's
written response, upon a member's request, interpreting the **Maryland
Attorneys' Rules of Professional Conduct** as applied to the inquirer's
contemplated conduct. The opinions are **advisory only** — they are not
binding on the Maryland Supreme Court or on the Attorney Grievance Commission
(which administers lawyer discipline). This makes them **doctrine**.

- **Series:** one numbered series `YYYY-NN` (e.g. `1987-34`, `2016-07`, `2025-02`)
- **Coverage:** ~498 opinions, 1987–present
- **Publisher:** Maryland State Bar Association (msba.org), Maryland's *voluntary* bar
- **Format:** clean HTML detail pages (no PDF, no OCR)

## How it works

1. **Discovery** — a single public index page,
   `/site/site/content/Resources-and-Tools-Content/Ethics-Opinions-and-Hotline.aspx`,
   lists every opinion as a link into
   `/Ethics-Opinions/{YEAR}/{YEAR}-{NN}.aspx`. There is no pagination; all
   ~498 links are on the one page.
2. **Extraction** — each detail page renders the opinion body in clean HTML
   inside the Decisis/SmartBar content panel
   `<div id="ste_container_ciOpinionTextBody_...">`, extracted directly with
   BeautifulSoup. The docket number is read from the page's
   `ETHICS DOCKET NO. YYYY-NN` header.
3. **Date** — MSBA dockets are numbered by year, so the date defaults to
   `YYYY-01-01`; an explicit in-range `Month DD, YYYY` in the body is used
   when present.

## Usage

```bash
python bootstrap.py test-api             # connectivity + extraction check
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # full pull (all opinions)
```

## Distinct from other Maryland sources

- **US/MD-EthicsOpinions** — Maryland *State Ethics Commission* (public
  officials / conflict-of-interest law; blocked, dsd.maryland.gov).
- **Maryland Judiciary Judicial Ethics Committee** — judges (not built here).
- **US/MD-COMAR** — Code of Maryland Regulations (legislation).
- **US/MD-Legislation** — Maryland statutes.

This source is the attorney professional-conduct advisory-opinion series
(lawyers), matching `US/{ST}-LegalEthics` in other states.

## License

Public Domain / freely published advisory opinions —
[MSBA Ethics Opinions & Hotline](https://www.msba.org/site/site/content/Resources-and-Tools-Content/Ethics-Opinions-and-Hotline.aspx).

MSBA Ethics Docket opinions are published free to the public on msba.org as an
educational service interpreting the Maryland Attorneys' Rules of Professional
Conduct. They are advisory (no disciplinary authority) and carry no login,
paywall or terms prohibiting reuse — treated as effectively public domain,
consistent with the other state-bar legal-ethics sources. Note the MSBA is
Maryland's **voluntary** bar, so the 17 U.S.C. § 105 government-edicts
rationale is weaker here than for an integrated bar; caveated like
US/NY-LegalEthics, US/IL-LegalEthics, US/CT-LegalEthics and US/VT-LegalEthics.
Commercial use: permitted.
