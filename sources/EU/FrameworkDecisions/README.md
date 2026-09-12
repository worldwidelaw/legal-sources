# EU/FrameworkDecisions — Framework Decisions & Joint Actions

The EU's former **third-pillar** binding acts, catalogued by the EU Publications
Office (CELLAR / EUR-Lex) under CELEX **sector 3, document class `F`**
(`3{YYYY}F{NNNN}`).

The `F` descriptor gathers the **Justice-and-Home-Affairs framework decisions**
(Art. 34 TEU — binding on Member States as to the result to be achieved, e.g. the
Framework Decision on the European Arrest Warrant 2002/584/JHA, on combating
terrorism 2002/475/JHA, on combating racism and xenophobia 2008/913/JHA, on the
European evidence warrant, on mutual recognition of financial penalties and of
custodial/probation sentences) together with the **joint actions** and **common
positions** adopted under Title VI TEU / early CFSP. The corpus spans the
mid-1990s to the present (~175 acts).

## Why this is additive

EU/EUR-Lex enumerates sector 3 by the ordinary **binding** resource-types only
(Regulations / Directives / Decisions and their implementing/delegated variants).
The `F` framework decisions and joint actions carry the dedicated
`FRAMEWORK_DEC` / `JOINT_ACT` resource types (not `DEC`), so EU/EUR-Lex never
pulls them. No `_id` collision with any other descriptor.

## How it works

1. **Enumerate** every act via the public CELLAR SPARQL endpoint (CELEX matching
   `^3[0-9]{4}F[0-9]`). The corpus is ~175 rows, far under the SPARQL OFFSET
   ceiling, so a single paged sweep suffices.
2. **Fetch full text** from CELLAR via HTTP content negotiation: OJ/Formex
   **xHTML** for modern acts, an **OJ HTML** manifestation via the
   language-suffixed CELEX (`/resource/celex/{CELEX}.ENG`) for older ones, and a
   born-digital **PDF** stream (PyMuPDF) for the remainder. CELLAR content
   negotiation bypasses the `eur-lex.europa.eu` AWS-WAF that 202-challenges
   datacenter IPs, so it is fleet-safe.
3. **Normalize** to the standard schema. `_type` is `legislation` (binding acts).

## Usage

```bash
python3 sources/EU/FrameworkDecisions/bootstrap.py test               # probe SPARQL + one act
python3 sources/EU/FrameworkDecisions/bootstrap.py bootstrap --sample # save 15 sample records
python3 sources/EU/FrameworkDecisions/bootstrap.py bootstrap-fast      # full corpus → data/records.jsonl
```

## License

[EU institutional reuse — Commission Decision 2011/833/EU](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32011D0833) — reuse permitted, including for commercial purposes; attribution to the source requested.
