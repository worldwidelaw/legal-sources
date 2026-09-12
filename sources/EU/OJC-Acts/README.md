# EU/OJC-Acts — Official Journal C-series Acts

Acts of the EU institutions published in the **Official Journal C series**
("Information and Notices"), catalogued by the EU Publications Office (CELLAR /
EUR-Lex) under CELEX **sector 3, document class `Y`** (`3{YYYY}Y{NNNN}`).

This is a large, heterogeneous body of authoritative **non-binding** acts and
official government-authored texts, including:

- **Council resolutions and conclusions** (the dominant category — strategic
  frameworks, sectoral policy resolutions, Council conclusions);
- **Commission communications and notices** published in the C series
  (state-aid notices, air-service public-service-obligation notices,
  anti-dumping notices, interpretative and explanatory communications);
- **European Court of Auditors special reports**;
- **Interinstitutional agreements** and **codes of conduct** between the
  institutions;
- **Agreements between the ECB and national central banks**, ECB/ESRB
  **guidelines** and warnings;
- the **rules of procedure** of Union bodies and the statutes of ERICs.

## Why this is additive

EU/EUR-Lex enumerates sector 3 by the **binding** resource-types only
(Regulations / Directives / Decisions and their implementing/delegated variants),
so it never pulls C-series `Y` acts. The small ESRB subset (~5%) partially
overlaps `EU/ESRB` (which keys on its own portal IDs, so the loader keeps both);
everything else — Council resolutions/conclusions, Commission C-series
communications, Court of Auditors special reports, interinstitutional agreements
— is not otherwise captured.

## How it works

1. **Enumerate** every C-series act via the public CELLAR SPARQL endpoint
   (CELEX matching `^3[0-9]{4}Y[0-9]`). The corpus is ~2,088 rows, under the
   ~10,000-row SPARQL OFFSET ceiling, so a single paged LIMIT/OFFSET sweep
   suffices (no year-scoping).
2. **Fetch full text** from CELLAR via HTTP content negotiation: OJ/Formex
   **xHTML** for modern acts, an **OJ HTML** manifestation via the
   language-suffixed CELEX (`/resource/celex/{CELEX}.ENG`) for older ones, and a
   born-digital **PDF** stream (PyMuPDF) for the remainder. CELLAR content
   negotiation bypasses the `eur-lex.europa.eu` AWS-WAF that 202-challenges
   datacenter IPs, so it is fleet-safe.
3. **Normalize** to the standard schema. `_type` is `doctrine` (non-binding
   soft-law / information acts).

## Usage

```bash
python3 sources/EU/OJC-Acts/bootstrap.py test              # probe SPARQL + one act
python3 sources/EU/OJC-Acts/bootstrap.py bootstrap --sample # save 15 sample records
python3 sources/EU/OJC-Acts/bootstrap.py bootstrap-fast     # full corpus → data/records.jsonl
```

## License

[EU institutional reuse — Commission Decision 2011/833/EU](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32011D0833) — reuse permitted, including for commercial purposes; attribution to the source requested.
