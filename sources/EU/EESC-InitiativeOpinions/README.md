# EU/EESC-InitiativeOpinions — EESC Own-Initiative & Exploratory Opinions

**Source:** [EU Publications Office (CELLAR)](http://publications.europa.eu/webapi/rdf/sparql) / [EUR-Lex](https://eur-lex.europa.eu)
**Data types:** doctrine

The European Economic and Social Committee (EESC) issues, alongside its referral
opinions (adopted on a mandatory/optional consultation on a specific legislative
file — CELEX descriptor `AE`, covered by **EU/EESC-Opinions**), a distinct class
of **own-initiative and exploratory opinions**, in which it takes up a subject on
its own motion or at the exploratory request of a presidency/institution. These
carry the separate CELEX descriptor `IE` (`5{YYYY}IE{NNNN}`, sector 5 = EESC/CoR
acts) and are published in the OJ C series.

This source is a distinct, additive companion to EU/EESC-Opinions: the two use
different CELEX descriptors (`AE` vs `IE`) so there is no document overlap, and
the loader dedups on the CELEX `_id` regardless.

## How it works

1. Enumerate every EESC own-initiative/exploratory opinion via the public CELLAR
   SPARQL endpoint (`FILTER REGEX(CELEX, "^5[0-9]{4}IE")`) → CELEX + document date
   + English title, paged with LIMIT/OFFSET.
2. Fetch full text via CELLAR HTTP content negotiation:
   `GET /resource/celex/{CELEX}` with `Accept: application/xhtml+xml` and
   `Accept-Language: en`. This serves the OJ Formex/xHTML body and bypasses the
   `eur-lex.europa.eu` AWS-WAF that 202-challenges datacenter IPs, so it is
   fleet-safe.
3. Normalize to the standard schema (doctrine).

The `IE` series spans 1973–present; full OJ text is available from ~1994 onward
(earlier acts are metadata-only and are skipped by the `< 200` char guard, with a
`MIN_YEAR = 1994` pre-filter to avoid needless fetches).

Same CELLAR recipe as EU/EESC-Opinions, EU/CoR-Opinions and EU/EP-Decisions,
narrowed to the `IE` descriptor.

## Usage

```bash
python3 sources/EU/EESC-InitiativeOpinions/bootstrap.py test            # probe SPARQL + one full text
python3 sources/EU/EESC-InitiativeOpinions/bootstrap.py bootstrap --sample
python3 sources/EU/EESC-InitiativeOpinions/bootstrap.py bootstrap-fast  # full corpus → data/records.jsonl
```

## License

[EU institutional reuse (Decision 2011/833/EU)](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32011D0833) — EESC opinions published in the Official Journal are reusable under the Commission's reuse policy; public-domain-equivalent for the OJ text. Attribution required; commercial use permitted.
