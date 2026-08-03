# US/WI-JudicialEthics — Wisconsin Judicial Conduct Advisory Committee (Formal Advisory Opinions)

Full text of the **formal advisory opinions** issued by the **Wisconsin Judicial
Conduct Advisory Committee**, a committee of the Supreme Court of Wisconsin that
advises judges and judicial officers on the compliance of their contemplated
conduct with the **Code of Judicial Conduct (SCR Chapter 60)**. Each opinion
(numbered `NN-N`) states the Issue, Answer, Facts, and Discussion.

- **Publisher:** Supreme Court of Wisconsin, Judicial Conduct Advisory Committee
- **Index:** https://www.wicourts.gov/supreme/sc_judcond.jsp
- **Coverage:** ~56 opinions
- **Type:** `doctrine` (official written interpretation of the judicial-conduct rules)
- **Full text:** born-digital PDFs with a real text layer. No OCR, no CAPTCHA, no auth.

This is **distinct** from `US/WI-EthicsOpinions`, which covers the executive
**Wisconsin Ethics Commission** (advisory opinions on the Code of Ethics for
public officials/employees and lobbying). This source covers the *judicial* ethics
committee (advice to judges under the Code of Judicial Conduct), part of the
project's judicial-ethics advisory-opinion vein (see also `US/WA-JudicialEthics`,
`US/FL-JudicialEthics`, `US/CT-JudicialEthics`).

## Access

1. GET the index page and parse each table row (Release date, `OPINION N-N: ...`
   description, and the View cell's `seqNo`).
2. For each opinion, fetch its PDF
   (`DisplayDocument.pdf?content=pdf&seqNo=NNNN`) and extract the full text via
   the shared PDF extractor (born-digital text layer). A `%PDF` magic-byte guard
   rejects the non-PDF 404 response.
3. The issue date is taken from the index Release-date column.

`wicourts.gov` returns HTTP 403 to non-browser User-Agents, so all requests carry
a desktop-Chrome UA.

## Usage

```bash
python bootstrap.py test-api             # Connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # Full pull (all opinions)
```

## License

[Public Domain (Wisconsin State Government Work)](https://www.law.cornell.edu/uscode/text/17/105) —
Formal advisory opinions of the Wisconsin Judicial Conduct Advisory Committee are
official public records of the State of Wisconsin court system, published for
public use with no copyright restriction. Commercial use permitted.
