# EU/InternationalAgreements — EU International Agreements

Full-text **international agreements** concluded by the European Union with third
countries and international organisations: trade and association agreements,
partnership and cooperation agreements, fisheries and air-transport agreements,
customs and mutual-assistance agreements, and the conventions of bodies set up by
such agreements. Their authentic text is published in the OJ L series.

This covers the **sector-2** CELEX `A` descriptor (`2{YYYY}A{NNNN}`, e.g.
`22026A00186` — the EU–Mercosur Partnership Agreement). It is a distinct,
additive corpus to [`EU/EUR-Lex`](../EUR-Lex), which covers sector-3 legislation
(regulations, directives, decisions); the agreements themselves live in sector 2
and are not otherwise pulled.

## Data access

- **Enumeration:** the public CELLAR SPARQL endpoint
  (`http://publications.europa.eu/webapi/rdf/sparql`), CELEX matching `^2[0-9]{4}A`,
  paged with LIMIT/OFFSET (~4,400 rows, well under the ~10 000-row ceiling — no
  year-scoping needed).
- **Full text:** CELLAR HTTP content negotiation on the CELEX —
  `Accept: application/xhtml+xml`, `Accept-Language: en` — serving the OJ
  Formex/xHTML body.

This path bypasses the `eur-lex.europa.eu` AWS-WAF that 202-challenges datacenter
IPs, so it is fleet-safe. Same CELLAR recipe as `EU/EESC-Opinions`,
`EU/EP-Decisions` and `EU/COM-Documents`, narrowed to the sector-2 `A` descriptor.
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
