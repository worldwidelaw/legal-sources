# US/NE-CIR — Nebraska Commission of Industrial Relations (CIR Reporter Decisions)

Full-text published decisions of the **Nebraska Commission of Industrial
Relations (CIR)**, formerly the **Court of Industrial Relations** — Nebraska's
independent quasi-judicial tribunal that adjudicates public-sector labor
disputes under Neb. Rev. Stat. Chapter 48, article 8:

- wage/benefit **comparability** determinations,
- bargaining-**unit determination** and **clarification**,
- **representation** controversies, and
- **prohibited / unfair labor practices**.

Each decision resolves a specific contested case, so every record is
`case_law`.

## Source

- Portal: <https://www.nebraska.gov/ncir/reporter_and_appeals_search/>
- Agency: <https://ncir.nebraska.gov/>

## How it works

The **CIR Reporter** is published online as bound volumes **1–19 (1948–2007)**.
Each volume index —

```
https://www.nebraska.gov/ncir/reporter_and_appeals_search/index.cgi?dir={V}_CIR_xx&type=reporter
```

— lists every decision in that volume as a direct link to a **born-digital
full-text HTML file**:

```
/ncir/reporter_and_appeals_search/data/reporter/{V}_CIR_xx/{V}_CIR_{page}_({year})_{caption}.html
```

These HTML files are the original decision text (FrontPage-authored, pure body
content — **no site chrome, no OCR, no JavaScript, no CAPTCHA, no auth**). The
scraper walks volumes 1–20, fetches each decision page, strips the HTML to clean
text, and parses:

- the reporter **citation** (volume / starting page / year) from the filename,
- the **case number** (`CASE NO. ...`) and **decision date** from the body.

~650 decisions total. Volumes 18–20 are sparse because recent decisions
(2008–present) have migrated to the modern `ncir.nebraska.gov/content/filings-opinions`
page as **scanned-image PDFs** (OCR-bound); those are out of scope for this
born-digital HTML index scrape.

### Commands

```bash
python bootstrap.py test-api            # connectivity + extraction check
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull (all reporter decisions)
```

## License

[Public Domain (US Government Work)](https://www.law.cornell.edu/uscode/text/17/105)
— decisions of the Nebraska Commission of Industrial Relations are official
state-government works in the public domain under the government-edicts
doctrine. Commercial use permitted; no attribution required.
