# EU/AG-Opinions — CJEU Opinions of the Advocates General

Opinions of the Advocates General of the Court of Justice of the European Union
(CJEU). Before the Court of Justice delivers judgment in most cases, an Advocate
General delivers a reasoned, independent **Opinion** proposing a legal solution.
These Opinions are substantial standalone legal analyses — frequently the fullest
account of the reasoning behind a point of EU law — and are published, with full
text, in the EU case-law collection.

- **Data type:** case_law (EUR-Lex classifies Advocate General Opinions in its
  case-law sector, alongside judgments and orders)
- **Coverage:** ~11,000 Opinions, 1961–present
- **Language:** English expression (where available)

## Access / recipe

Each Opinion carries a CELEX number of the form `6{YYYY}CC{NNNN}`
(sector 6 = EU case-law; `CC` = *conclusions* / Opinion of an Advocate General;
`YYYY` = case-registration year).

1. **Enumerate** via the public CELLAR SPARQL endpoint
   (`http://publications.europa.eu/webapi/rdf/sparql`), matching
   `^6{YYYY}CC[0-9]+$`. Enumeration is scoped **one case-year at a time** because
   the full corpus exceeds the CELLAR SPARQL 10,000-row `OFFSET` ceiling.
2. **Fetch full text** via CELLAR HTTP content negotiation:
   `GET http://publications.europa.eu/resource/celex/{CELEX}` with
   `Accept: application/xhtml+xml` (modern Opinions) or `Accept: text/html`
   (legacy fallback for older Opinions), `Accept-Language: en`. This serves the
   Formex/xHTML (or legacy HTML) body and bypasses the eur-lex.europa.eu AWS-WAF
   that 202-challenges datacenter IPs.

Sibling of **EU/CURIA** (CJEU/General-Court *judgments*, CELEX type `CJ`/`TJ`)
and of **EU/EESC-Opinions** / **EU/CoR-Opinions** (same CELLAR
content-negotiation recipe for OJ C-series advisory opinions).

## Usage

```bash
python3 sources/EU/AG-Opinions/bootstrap.py test              # SPARQL + one full-text probe
python3 sources/EU/AG-Opinions/bootstrap.py bootstrap --sample  # 15 sample records
python3 sources/EU/AG-Opinions/bootstrap.py bootstrap         # full corpus
python3 sources/EU/AG-Opinions/bootstrap.py update            # incremental (by document date)
```

## License

[EU institutional reuse — Commission Decision 2011/833/EU](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32011D0833) — reuse permitted, attribution required. Commercial use permitted. The Official Journal / case-law text is public-domain-equivalent.
