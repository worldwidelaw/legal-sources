# US/CT-JudicialEthics — Connecticut Committee on Judicial Ethics (Informal Opinion Summaries)

Full text of the **Informal Opinion Summaries** issued by the **Connecticut
Committee on Judicial Ethics**, a committee of the Connecticut Judicial Branch
that advises judges and judicial officials on the **Connecticut Code of Judicial
Conduct**. Each opinion (numbered `YYYY-NN`) answers a specific inquiry, stating
the facts, the issue, the relevant Rules/Canons, and the Committee's conclusion.

- **Publisher:** State of Connecticut Judicial Branch, Committee on Judicial Ethics
- **Index:** https://www.jud.ct.gov/committees/ethics/summaries.htm
- **Coverage:** ~344 opinions, 2008–present
- **Type:** `doctrine` (official written interpretation of the judicial-conduct rules)
- **Full text:** born-digital — older opinions are HTML pages (cp1252), newer ones
  are PDFs with a real text layer. No OCR, no CAPTCHA, no auth.

This is **distinct** from `US/CT-EthicsOpinions`, which covers the executive-branch
**Office of State Ethics** (advisory opinions on the Code of Ethics for public
officials/state employees). This source covers the *judicial* ethics committee
(advice to judges under the Code of Judicial Conduct), part of the project's
judicial-ethics advisory-opinion vein (see also `US/WA-JudicialEthics`,
`US/FL-JudicialEthics`).

## Access

1. GET the summaries index and enumerate every `sum/YYYY-NN.(htm|pdf)` link,
   deduped by opinion number.
2. For each opinion, fetch its document and extract the full text:
   - **HTML** pages are decoded (utf-8 → cp1252 fallback) and de-tagged.
   - **PDF** opinions are extracted via the shared PDF text extractor
     (born-digital text layer).
3. The issue date is parsed from the `(Month DD, YYYY)` header parenthetical.

`jud.ct.gov` returns HTTP 403 to non-browser User-Agents, so all requests carry a
desktop-Chrome UA.

## Usage

```bash
python bootstrap.py test-api             # Connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # Full pull (all opinions)
```

## License

[Public Domain (Connecticut State Government Work)](https://www.law.cornell.edu/uscode/text/17/105) —
Informal Opinion Summaries of the Connecticut Committee on Judicial Ethics are
official public records of the State of Connecticut Judicial Branch, published for
public use with no copyright restriction. Commercial use permitted.
