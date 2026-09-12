# EU/EP-Decisions — European Parliament Decisions

Full-text **decisions** adopted by the European Parliament in plenary, beyond its
legislative positions ([`EU/EP-Positions`](../EP-Positions), `AP`) and
non-legislative resolutions ([`EU/EP-Resolutions`](../EP-Resolutions), `IP`):
appointments and renewals of members of EU bodies (Court of Auditors, ECB
Executive Board, EU agencies), decisions on the waiver or defence of a Member's
parliamentary immunity, decisions not to object to Commission
delegated/implementing acts, discharge and own-resources decisions, and similar
procedural acts (`P{n}_TA(YYYY)NNNN` texts-adopted identifiers).

This covers the sector-5 CELEX `DP` descriptor (`5{YYYY}DP{NNNN}`), spanning
1999–present. It is a distinct, additive companion to `EU/EuroParl`, whose
adopted-texts API only reaches back to 2014 — the DP series' pre-2014 half is
cleanly additive (the loader dedups on the CELEX `_id`).

## Data access

- **Enumeration:** the public CELLAR SPARQL endpoint
  (`http://publications.europa.eu/webapi/rdf/sparql`), CELEX matching `^5[0-9]{4}DP`,
  paged with LIMIT/OFFSET (the corpus is well under the ~10 000-row ceiling).
- **Full text:** CELLAR HTTP content negotiation on the CELEX —
  `Accept: application/xhtml+xml`, `Accept-Language: en` — serving the OJ
  Formex/xHTML body.

This path bypasses the `eur-lex.europa.eu` AWS-WAF that 202-challenges datacenter
IPs, so it is fleet-safe. Same CELLAR recipe as `EU/EESC-Opinions`,
`EU/CoR-Opinions` and `EU/EP-Resolutions`, narrowed to the `DP` descriptor.

## Usage

```bash
python3 bootstrap.py test               # connectivity check
python3 bootstrap.py bootstrap --sample   # write 15 sample records
python3 bootstrap.py bootstrap          # full corpus -> data/records.jsonl
```

## Record schema

`doctrine` records with `_id` (CELEX), `title`, full `text`, `date`, `url`.

## License

[EU institutional reuse — Decision 2011/833/EU](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32011D0833) — documents published by the EU Publications Office are reusable, including for commercial purposes, with attribution to the source requested.
