# US/TX-JudicialEthics — Texas Committee on Judicial Ethics (Judicial Ethics Opinions)

Full text of the **judicial ethics opinions** issued by the **Committee on
Judicial Ethics of the State Bar of Texas Judicial Section**. The Committee
renders written opinions interpreting the **Texas Code of Judicial Conduct** in
response to written inquiries from judges; the General Counsel of the Office of
Court Administration (OCA) publishes them. Each opinion states the topic,
QUESTION, ANSWER, and DISCUSSION.

- **Publisher:** Office of Court Administration, Texas Judicial Branch (for the
  Committee on Judicial Ethics, State Bar of Texas Judicial Section)
- **Index page:** https://www.txcourts.gov/publications-training/judicial-ethics-bench-books/judicial-ethics-opinions.aspx
- **Compilation PDF:** https://www.txcourts.gov/media/678096/JudicialEthicsOpinions.pdf
- **Coverage:** ~289 opinions, Nos. 1–296 (1975–2013)
- **Type:** `doctrine` (official written interpretation of the judicial-conduct rules)
- **Full text:** born-digital PDF compilation with a real text layer. No OCR, no CAPTCHA, no auth.

This is **distinct** from `US/TX-EthicsOpinions`, which covers the executive
**Texas Ethics Commission** (advisory opinions on the Government Code for public
officials, candidates, and lobbyists). This source covers the *judicial* ethics
committee (advice to judges under the Code of Judicial Conduct), part of the
project's judicial-ethics advisory-opinion vein (see also `US/WA-JudicialEthics`,
`US/FL-JudicialEthics`, `US/CT-JudicialEthics`, `US/WI-JudicialEthics`).

## Access

1. Obtain the consolidated PDF (`media/678096/JudicialEthicsOpinions.pdf`). The
   whole corpus lives in this one born-digital file.
2. Extract the text layer, strip the running `Texas Judicial Ethics Opinions /
   Page N of NNN` footer, and split on the per-opinion `Opinion No. N (YYYY)`
   headers (each alone on its own line; inline citations to other opinions are
   excluded).
3. The ALL-CAPS topic caption printed just above each header is the opinion
   title; the year in the header parenthesis is the date.

`txcourts.gov` is behind an Azure Front Door WAF that returns HTTP 403 to
datacenter / non-residential IPs. The scraper tries the live host first (with a
desktop-Chrome UA) and, on a WAF block, falls back to the latest Internet-Archive
Wayback capture of the same PDF (refreshed several times a year), so the corpus
stays reachable and current from any vantage.

## Usage

```bash
python bootstrap.py test-api             # Connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # Full pull (all opinions)
```

## License

[Public Domain (Texas State Government Work)](https://www.law.cornell.edu/uscode/text/17/105) —
Texas judicial ethics opinions are official public records published by the
Office of Court Administration of the Texas Judicial Branch for public use with
no copyright restriction. Commercial use permitted.
