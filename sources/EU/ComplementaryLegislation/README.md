# EU — Complementary Legislation

Full-text of the European Union's **complementary legislation** — the fourth
sector of the EUR-Lex document taxonomy. These are binding acts published in the
Official Journal that are not secondary legislation of a single institution, and
are not otherwise pulled by EU/EUR-Lex (sector 3), EU/InternationalAgreements
(sector 2 A) or EU/JointBodyDecisions (sector 2 D).

The corpus (~1,650 acts) is dominated by three classes:

- **UN/UNECE Regulations** (CELEX descriptor `X`) — uniform technical
  prescriptions for the type-approval of motor vehicles, their equipment and
  parts (braking, lighting, safety-glazing, emissions, etc.), given legal effect
  in the EU through its accession to the UNECE 1958 Agreement. Substantial,
  directly applicable regulatory texts.
- **Decisions of the Representatives of the Governments of the Member States**
  meeting within the Council (descriptor `D`) — e.g. appointing Judges and
  Advocates-General to the Court of Justice and the General Court.
- **Resolutions of the Council and of the Representatives of the Governments of
  the Member States** meeting within the Council (descriptor `Y`).

## How it works

1. Enumerate every sector-4 act via the public **CELLAR SPARQL endpoint**
   (`http://publications.europa.eu/webapi/rdf/sparql`), filtering CELEX numbers
   matching `^4[0-9]{4}`. The corpus is well under the ~10 000-row SPARQL OFFSET
   ceiling, so simple LIMIT/OFFSET paging suffices (no year-scoping).
2. Fetch each act's full text from CELLAR via HTTP **content negotiation**:
   `GET http://publications.europa.eu/resource/celex/{CELEX}` with
   `Accept: application/xhtml+xml` and `Accept-Language: en`. This serves the OJ
   Formex/xHTML body and **bypasses the eur-lex.europa.eu AWS-WAF** that
   202-challenges datacenter IPs, so it runs from the fleet.
3. Normalize to the standard schema (`_type: legislation`).

Corrigenda (`…R(01)`) and metadata-only stubs have no xHTML manifestation
(HTTP 404) and are skipped by the minimum-length guard.

## Usage

```bash
# 15-record validation sample
python3 sources/EU/ComplementaryLegislation/bootstrap.py bootstrap --sample

# quick connectivity check
python3 sources/EU/ComplementaryLegislation/bootstrap.py test

# full corpus (streams to data/records.jsonl)
python3 sources/EU/ComplementaryLegislation/bootstrap.py bootstrap-fast
```

## License

[EU institutional reuse — Commission Decision 2011/833/EU](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32011D0833) — documents published by the EU Publications Office are reusable, including for commercial purposes, provided the source is acknowledged. Content is retrieved from the official CELLAR repository.
