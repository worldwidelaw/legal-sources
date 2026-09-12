# EU — Treaties (Primary Law)

Full-text of the **primary law of the European Union** — the first sector of the
EUR-Lex document taxonomy. This is the EU's constitutional layer and is not
otherwise pulled by EU/EUR-Lex (sector 3, secondary legislation) or the sector-2
/ sector-4 siblings.

The corpus (~9,800 works) includes:

- The consolidated **Treaty on European Union** (TEU, `12016M/TXT`) and **Treaty
  on the Functioning of the European Union** (TFEU, `12016E/TXT`).
- The **Charter of Fundamental Rights of the European Union** (`12016A/TXT`).
- The **Euratom Treaty** and the successive amending Treaties — Maastricht,
  Amsterdam, Nice and **Lisbon** (`12007L/TXT`).
- The **Accession Treaties** by which new Member States joined the Union.
- The **Protocols** and **Declarations** annexed to the Treaties.
- The **UK Withdrawal Agreement** (`12020W/TXT`) and its annexed declarations.

## How it works

1. Enumerate every sector-1 act via the public **CELLAR SPARQL endpoint**
   (`http://publications.europa.eu/webapi/rdf/sparql`), filtering CELEX numbers
   matching `^1[0-9]{4}`. The corpus is under the ~10 000-row SPARQL OFFSET
   ceiling, so simple LIMIT/OFFSET paging suffices (no year-scoping).
2. Fetch each act's full text from CELLAR via HTTP **content negotiation**:
   `GET http://publications.europa.eu/resource/celex/{CELEX}` with
   `Accept: application/xhtml+xml` and `Accept-Language: en`. Sector-1 CELEX
   numbers carry `/` and parenthesised sub-part markers (e.g. `12020W/TXT`,
   `12019W/TXT(02)`), so the CELEX is **URL-encoded** before it is appended to
   the resource path. This serves the OJ Formex/xHTML body and **bypasses the
   eur-lex.europa.eu AWS-WAF** that 202-challenges datacenter IPs, so it runs
   from the fleet.
3. Normalize to the standard schema (`_type: legislation`).

Corrigenda (`…R(0N)`) and manifestations with no xHTML export return HTTP 404 and
are skipped by the minimum-length guard.

## Usage

```bash
# 15-record validation sample
python3 sources/EU/Treaties/bootstrap.py bootstrap --sample

# quick connectivity check
python3 sources/EU/Treaties/bootstrap.py test

# full corpus (streams to data/records.jsonl)
python3 sources/EU/Treaties/bootstrap.py bootstrap-fast
```

## License

[EU institutional reuse — Commission Decision 2011/833/EU](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32011D0833) — documents published by the EU Publications Office are reusable, including for commercial purposes, provided the source is acknowledged. Content is retrieved from the official CELLAR repository.
