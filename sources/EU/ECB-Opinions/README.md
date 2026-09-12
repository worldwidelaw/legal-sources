# EU/ECB-Opinions — Opinions of the European Central Bank

Full text of the **Opinions of the European Central Bank** (and, for 1994–1998,
its predecessor the **European Monetary Institute**).

Under Article 127(4) and Article 282(5) TFEU, the ECB must be consulted on any
proposed Union act, and on any national draft legislation, within its fields of
competence (monetary policy, payment and settlement systems, banknotes,
statistics, prudential supervision, etc.). The resulting opinions — reference
`CON/YYYY/NN` — are published in the OJ C series and in the ECB's public
register of opinions.

- CELEX `5{YYYY}AB{NNNN}` (sector 5 = preparatory acts; `AB` = the central-bank-opinion class)
- ~1,700 documents, 1994–present

This is the ECB-side counterpart to the Commission proposals in
**EU/COM-Documents**, the Council positions in **EU/Council-Positions** and the
EESC/CoR opinions in **EU/EESC-Opinions** / **EU/CoR-Opinions**.

## How it works

1. **Enumeration** — the public CELLAR SPARQL endpoint
   (`http://publications.europa.eu/webapi/rdf/sparql`) lists every ECB/EMI
   opinion, filtered by a *year-scoped* CELEX regex `^5{YYYY}AB`. Year-scoping
   keeps each query small so it never hits the ~10K SPARQL `OFFSET` ceiling.
2. **Full text** via CELLAR HTTP content negotiation
   (`http://publications.europa.eu/resource/celex/{CELEX}`):
   - The usual case: CELLAR answers `Accept: application/pdf` with a direct
     born-digital **PDF**, extracted with PyMuPDF (no OCR).
   - A minority carry an OJ/Formex **xHTML** manifestation
     (`Accept: application/xhtml+xml`) → tags stripped.
   - Where CELLAR returns a `300 Multiple-Choice` listing of PDF streams, we
     pick the English "part1" PDF (fallback: first English PDF, then `DOC_1`).

CELLAR content negotiation bypasses the `eur-lex.europa.eu` AWS-WAF that
202-challenges datacenter IPs, so this source is **fleet-safe**.

This is the same recipe as the sibling sources **EU/COM-Documents**,
**EU/SWD-Documents** and **EU/Council-Positions**, narrowed to the `AB`
descriptor. Empty-body corrigendum sub-parts fall below the 200-char text floor
and are skipped.

## Data type

`doctrine` — official ECB preparatory/consultative documents (consistent with
the COM-Documents, SWD-Documents, Council-Positions and EESC/CoR opinion
sources).

## Usage

```bash
python3 bootstrap.py test               # SPARQL + fetch smoke test
python3 bootstrap.py bootstrap --sample # write 15 sample records
python3 bootstrap.py bootstrap          # full corpus → data/records.jsonl
python3 bootstrap.py update             # rescan recent years for new docs
```

## License

[EU institutional reuse — Commission Decision 2011/833/EU](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32011D0833) — reuse of ECB/Commission documents authorised, attribution requested. Commercial use permitted.
