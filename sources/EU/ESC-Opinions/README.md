# EU/ESC-Opinions — Economic and Social Committee Opinions (historical AC series)

Full-text advisory opinions of the **Economic and Social Committee** (ESC/CES) —
the EU's advisory body of employers, workers and other civil-society interests,
created by the 1957 Treaty of Rome and renamed the **European** Economic and
Social Committee (EESC) in 2002. The Committee adopts opinions on Commission
proposals and own-initiative topics, published in the OJ C series.

This source covers the **historical `AC` descriptor** (CELEX `5{YYYY}AC{NNNN}`),
used up to ~2000, and is a distinct, additive companion to
[`EU/EESC-Opinions`](../EESC-Opinions), which covers the modern `AE` descriptor.
The `AE` series is effectively empty before 2000, so `AC` is the authoritative
descriptor for the Committee's opinions through the 1990s.

## Data access

- **Enumeration:** the public CELLAR SPARQL endpoint
  (`http://publications.europa.eu/webapi/rdf/sparql`), CELEX matching
  `^5{YEAR}AC`, one publication year at a time (1993–2000) to stay under the
  ~10 000-row SPARQL OFFSET ceiling.
- **Full text:** CELLAR HTTP content negotiation. The historical AC opinions are
  served as an **OJ HTML** manifestation addressed by the language-suffixed CELEX
  (`/resource/celex/{CELEX}.ENG`, `Accept: text/html`), with xHTML and
  born-digital PDF (PyMuPDF) fallbacks.

CELLAR only holds a digitised full-text body for AC opinions from ~1995 onward
(earlier years are metadata-only bibliographic notices); the scraper keeps only
records that resolve to real full text (≥ 200 chars).

This path bypasses the `eur-lex.europa.eu` AWS-WAF that 202-challenges datacenter
IPs, so it is fleet-safe. Same CELLAR recipe as `EU/EESC-Opinions`,
`EU/CoR-Opinions` and `EU/ECB-Opinions`, narrowed to the `AC` descriptor.

## Usage

```bash
python3 bootstrap.py test              # connectivity check
python3 bootstrap.py bootstrap --sample  # write 15 sample records
python3 bootstrap.py bootstrap         # full corpus -> data/records.jsonl
```

## Record schema

`doctrine` records with `_id` (CELEX), `title`, full `text`, `date`, `url`.

## License

[EU institutional reuse — Decision 2011/833/EU](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32011D0833) — documents published by the EU Publications Office are reusable, including for commercial purposes, with attribution to the source requested.
