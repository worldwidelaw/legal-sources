# US/KS-AGOpinions — Kansas Attorney General Opinions

Full text of formal legal opinions issued by the **Kansas Attorney General**.
Each opinion answers a legal question posed by a public official (legislators,
state agencies, county/city officials) and constitutes an authoritative
(advisory) interpretation of Kansas law — classified here as **doctrine**.

## Source

- **Archive host:** Washburn University School of Law Library —
  <https://ksag.washburnlaw.edu/>
- **Issuing authority:** Office of the Kansas Attorney General (ag.ks.gov)
- **Coverage:** 1974 – present (several thousand opinions)
- **Format:** digitally-produced text PDFs, one directory per year
  (`/opinions/{YEAR}/{YEAR}-NNN.pdf`)

The Kansas AG's own website (`ag.ks.gov`) bot-blocks automated access
(HTTP 403). The Washburn Law Library hosts the complete corpus as an open,
static, full-text archive, which is the source used here.

## How it works

1. Walk year directories `/opinions/{YEAR}/` for `YEAR` from the current
   year back to 1974.
2. Each year index lists its opinions as `{YEAR}-NNN.pdf` links.
3. Download each PDF and extract its text via the shared
   `common.pdf_extract.extract_pdf_markdown` helper (OOM-hardened). The
   PDFs carry a real text layer, so no OCR is required.
4. The issued date is parsed from the opinion's opening line
   (`Month D, YYYY`), falling back to year-only.

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction test
python bootstrap.py bootstrap --sample  # ~12 sample opinions
python bootstrap.py bootstrap           # full pull (all years)
```

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) —
Kansas Attorney General opinions are official state government works in the
public domain under the government-edicts doctrine. Commercial use permitted,
no attribution required. The Washburn archive is an open full-text host, not a
rights holder.
