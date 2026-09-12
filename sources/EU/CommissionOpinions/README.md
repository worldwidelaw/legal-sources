# EU — Commission Opinions

Full text of the **opinions of the European Commission** — the non-binding acts
of Article 288 TFEU ("opinions … shall have no binding force"). Though not
legally binding, they are authoritative instruments that the Commission is
required or empowered to issue under specific Treaty and secondary-law bases, and
they are published in the Official Journal. Examples include:

- Commission opinions on the **draft budgetary plans** of euro-area Member States
  under Regulation (EU) No 473/2013 (the "two-pack");
- opinions relating to plans for the **disposal of radioactive waste** under
  Article 37 of the Euratom Treaty;
- opinions on requests to **amend the statutes of national central banks**;
- opinions on **draft amendments to the Treaties** under Article 48 TEU;
- opinions on **accession** applications and other institutional matters.

They occupy CELEX **sector 3** (secondary legislation of the institutions),
document class **`A`** — `3{YYYY}A{NNNN}` — ~655 acts. This is a distinct,
additive corpus: EU/EUR-Lex enumerates sector 3 by the **binding** resource-types
only (REG / DIR / DEC and their implementing/delegated variants) and therefore
never pulls Commission opinions; the advisory-body opinions (EESC / CoR / ECB)
live under their own sector-5 descriptors (`AE` / `AR` / `AB`).

## How it works

1. Enumerate every opinion via the public **CELLAR SPARQL endpoint**
   (`http://publications.europa.eu/webapi/rdf/sparql`), filtering CELEX numbers
   matching `^3[0-9]{4}A[0-9]`. The corpus is well under the ~10 000-row SPARQL
   OFFSET ceiling, so a single LIMIT/OFFSET sweep suffices (no year-scoping).
2. Fetch each act's full text from CELLAR via HTTP **content negotiation**,
   trying three manifestations in turn (all served by CELLAR, which **bypasses
   the eur-lex.europa.eu AWS-WAF** that 202-challenges datacenter IPs, so it runs
   from the fleet):
   - **xHTML** (OJ/Formex body) — `Accept: application/xhtml+xml` — the usual
     case for modern opinions;
   - **OJ HTML** via the language-suffixed CELEX
     (`GET .../resource/celex/{CELEX}.ENG`, `Accept: text/html`) — for older
     opinions that predate the Formex export;
   - **born-digital PDF** (`Accept: application/pdf`, extracted with PyMuPDF) —
     for the remainder.
3. Normalize to the standard schema (`_type: doctrine` — non-binding soft law).

Corrigenda (`…R(01)`) and metadata-only stubs resolve to no usable manifestation
and are skipped by the minimum-length guard.

## Usage

```bash
# 15-record validation sample
python3 sources/EU/CommissionOpinions/bootstrap.py bootstrap --sample

# quick connectivity check
python3 sources/EU/CommissionOpinions/bootstrap.py test

# full corpus (streams to data/records.jsonl)
python3 sources/EU/CommissionOpinions/bootstrap.py bootstrap-fast
```

## License

[EU institutional reuse — Commission Decision 2011/833/EU](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32011D0833) — documents published by the EU Publications Office are reusable, including for commercial purposes, provided the source is acknowledged. Content is retrieved from the official CELLAR repository.
