# EU — Recommendations

Full text of the European Union's **recommendations** — the non-binding acts of
Article 288 TFEU ("recommendations … shall have no binding force"). Though not
legally binding, they are authoritative soft-law instruments of the Council, the
Commission and the European Central Bank, and are heavily relied on in practice:
the Commission Recommendation defining micro, small and medium-sized enterprises
(2003/361/EC), the country-specific recommendations of the European Semester,
recommendations on minimum income, on cybersecurity, on responsible business
conduct, on the rule of law, and many more.

They occupy CELEX **sector 3** (secondary legislation of the institutions),
document class **`H`** — `3{YYYY}H{NNNN}` — and span 1960 to the present
(~1,770 acts). This is a distinct, additive corpus: EU/EUR-Lex enumerates sector
3 by the **binding** resource-types only (REG / DIR / DEC and their
implementing/delegated variants) and therefore never pulls recommendations.

## How it works

1. Enumerate every recommendation via the public **CELLAR SPARQL endpoint**
   (`http://publications.europa.eu/webapi/rdf/sparql`), filtering CELEX numbers
   matching `^3[0-9]{4}H[0-9]`. The corpus is well under the ~10 000-row SPARQL
   OFFSET ceiling, so a single LIMIT/OFFSET sweep suffices (no year-scoping).
2. Fetch each act's full text from CELLAR via HTTP **content negotiation**,
   trying three manifestations in turn (all served by CELLAR, which **bypasses
   the eur-lex.europa.eu AWS-WAF** that 202-challenges datacenter IPs, so it runs
   from the fleet):
   - **xHTML** (OJ/Formex body) — `Accept: application/xhtml+xml` — the usual
     case for modern recommendations;
   - **OJ HTML** via the language-suffixed CELEX
     (`GET .../resource/celex/{CELEX}.ENG`, `Accept: text/html`) — for older
     recommendations that predate the Formex export;
   - **born-digital PDF** (`Accept: application/pdf`, extracted with PyMuPDF) —
     for the remainder.
3. Normalize to the standard schema (`_type: doctrine` — non-binding soft law).

Corrigenda (`…R(01)`) and metadata-only stubs resolve to no usable manifestation
and are skipped by the minimum-length guard.

## Usage

```bash
# 15-record validation sample
python3 sources/EU/Recommendations/bootstrap.py bootstrap --sample

# quick connectivity check
python3 sources/EU/Recommendations/bootstrap.py test

# full corpus (streams to data/records.jsonl)
python3 sources/EU/Recommendations/bootstrap.py bootstrap-fast
```

## License

[EU institutional reuse — Commission Decision 2011/833/EU](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32011D0833) — documents published by the EU Publications Office are reusable, including for commercial purposes, provided the source is acknowledged. Content is retrieved from the official CELLAR repository.
