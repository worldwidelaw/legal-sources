# EU/JointBodyDecisions — Decisions of Bodies Established by EU International Agreements

Full-text **decisions** taken to administer and extend the European Union's
international agreements. The corpus is dominated by decisions of the joint bodies
set up by those agreements — above all the **EEA Joint Committee** (which
incorporates EU legislation into the EEA Agreement, extending it to Norway,
Iceland and Liechtenstein) — together with **Association Council** and
**Cooperation Council** decisions under the EU's association/partnership
agreements, plus Council decisions in external relations.

This covers the **sector-2** CELEX `D` descriptor (`2{YYYY}D{NNNN}`, e.g.
`22026D1299` — EEA Joint Committee Decision No 107/2026). It is a distinct,
additive corpus to [`EU/EUR-Lex`](../EUR-Lex) (sector-3 legislation) and
[`EU/InternationalAgreements`](../InternationalAgreements) (sector-2 `A` agreement
texts) — these decisions live in sector-2 `D` and are not otherwise pulled.

## Data access

- **Enumeration:** the public CELLAR SPARQL endpoint
  (`http://publications.europa.eu/webapi/rdf/sparql`), CELEX matching `^2[0-9]{4}D`,
  paged with LIMIT/OFFSET (~8,900 rows, under the ~10 000-row ceiling — no
  year-scoping needed).
- **Full text:** CELLAR HTTP content negotiation on the CELEX —
  `Accept: application/xhtml+xml`, `Accept-Language: en` — serving the OJ
  Formex/xHTML body.

This path bypasses the `eur-lex.europa.eu` AWS-WAF that 202-challenges datacenter
IPs, so it is fleet-safe. Same CELLAR recipe as `EU/InternationalAgreements`,
`EU/EESC-Opinions` and `EU/EP-Decisions`, narrowed to the sector-2 `D` descriptor.
Corrigenda (`…R(01)`) have no xHTML manifestation (404) and are skipped by the
minimum-length guard.

## Usage

```bash
python3 bootstrap.py test                 # connectivity check
python3 bootstrap.py bootstrap --sample   # write 15 sample records
python3 bootstrap.py bootstrap            # full corpus -> data/records.jsonl
```

## Record schema

`legislation` records with `_id` (CELEX), `title`, full `text`, `date`, `url`.

## License

[EU institutional reuse — Decision 2011/833/EU](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32011D0833) — documents published by the EU Publications Office are reusable, including for commercial purposes, with attribution to the source requested.
