# US/MA-JudicialEthics — Massachusetts Committee on Judicial Ethics (CJE Opinions)

Full text of the **judicial ethics opinions** ("Letter Opinions") issued by the
**Massachusetts Committee on Judicial Ethics (CJE)**, a committee of the
**Massachusetts Supreme Judicial Court** that advises judges on the application
of the **Massachusetts Code of Judicial Conduct**. Each opinion states the
subject, facts, and the Committee's advice.

- **Publisher:** Massachusetts Supreme Judicial Court, Committee on Judicial Ethics
- **Chronological index:** https://www.mass.gov/info-details/chronological-index-of-judicial-ethics-opinions
- **Opinion page:** https://www.mass.gov/opinion/cje-opinion-no-{number}
- **Coverage:** ~244 opinions, 1989–present
- **Type:** `doctrine` (official written interpretation of the judicial-conduct rules)
- **Full text:** born-digital HTML pages. No OCR, no CAPTCHA, no auth.

This is **distinct** from `US/MA-EthicsOpinions`, which covers the executive
**Massachusetts State Ethics Commission** (EC-COI conflict-of-interest opinions
for public employees under G.L. c. 268A). This source covers the *judicial*
ethics committee (advice to judges under the Code of Judicial Conduct), part of
the project's judicial-ethics advisory-opinion vein (see also
`US/WA-JudicialEthics`, `US/FL-JudicialEthics`, `US/CT-JudicialEthics`,
`US/WI-JudicialEthics`, `US/TX-JudicialEthics`).

## Access

1. GET the Chronological Index and collect every `/opinion/cje-opinion-no-...`
   href.
2. For each opinion, fetch its page and extract the body from the
   `div.ma__rich-text` container (the longest one on the page). The subject
   heading and the `Date: MM/DD/YYYY` field are taken from the same page.
3. Opinions withheld from publication (page contains "decided not to publish")
   are skipped — they carry no full text.

`mass.gov` (Akamai) serves the full server-side HTML only to a **plain
(non-browser) User-Agent**; a browser UA gets a JS shell. All requests therefore
use a plain `python-requests` UA (the same inversion used by
`US/MA-EthicsOpinions`).

## Usage

```bash
python bootstrap.py test-api             # Connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # Full pull (all opinions)
```

## License

[Public Domain (Massachusetts State Government Work)](https://www.law.cornell.edu/uscode/text/17/105) —
Massachusetts judicial ethics opinions are official public records of the
Massachusetts Supreme Judicial Court, published on mass.gov for public use with
no copyright restriction. Commercial use permitted.
