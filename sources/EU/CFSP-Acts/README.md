# EU/CFSP-Acts — CFSP Joint Actions & Common Positions

The EU's **second-pillar** Common Foreign and Security Policy (CFSP, Title V TEU)
acts, catalogued by the EU Publications Office (CELLAR / EUR-Lex) under CELEX
**sector 3, document class `E`** (`3{YYYY}E{NNNN}`).

The `E` descriptor gathers the Council **joint actions** and **common positions**
(and the earlier CFSP decisions carrying this class) adopted from the mid-1990s
until the Lisbon Treaty reorganised CFSP instruments. They cover **restrictive
measures / sanctions** against third countries and persons, the mandates of
**ESDP/CSDP crisis-management missions and operations** (EUFOR, EUPOL, EU Special
Representatives, EUNAVFOR Atalanta, etc.), **non-proliferation and arms-control**
actions, and the amendments/extensions/repeals of those instruments. Example:
`32009E0788` = Common Position 2009/788/CFSP. The corpus spans ~667 acts.

## Why this is additive

EU/EUR-Lex enumerates sector 3 by the ordinary **binding** resource-types only
(Regulations / Directives / Decisions and their implementing/delegated variants).
The `E` CFSP joint actions and common positions carry the dedicated
`JOINT_ACTION` / `COMMON_POSITION` resource types (not `DEC`), so EU/EUR-Lex never
pulls them. Distinct too from **EU/FrameworkDecisions** (descriptor `F` = the
third-pillar / JHA framework decisions) — `E` and `F` are different CELEX letters,
so there is no `_id` collision with any descriptor.

## How it works

1. **Enumerate** every act via the public CELLAR SPARQL endpoint (CELEX matching
   `^3[0-9]{4}E[0-9]`). The corpus is ~667 rows, far under the SPARQL OFFSET
   ceiling, so a single paged sweep suffices.
2. **Fetch full text** from CELLAR via HTTP content negotiation: OJ/Formex
   **xHTML** for modern acts, an **OJ HTML** manifestation via the
   language-suffixed CELEX (`/resource/celex/{CELEX}.ENG`) for older ones, and a
   born-digital **PDF** stream (PyMuPDF) for the remainder. CELLAR content
   negotiation bypasses the `eur-lex.europa.eu` AWS-WAF that 202-challenges
   datacenter IPs, so it is fleet-safe.
3. **Normalize** to the standard schema. `_type` is `legislation` (binding CFSP
   acts).

## Usage

```bash
python3 sources/EU/CFSP-Acts/bootstrap.py test               # probe SPARQL + one act
python3 sources/EU/CFSP-Acts/bootstrap.py bootstrap --sample # save 15 sample records
python3 sources/EU/CFSP-Acts/bootstrap.py bootstrap-fast      # full corpus → data/records.jsonl
```

## License

[EU institutional reuse — Commission Decision 2011/833/EU](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32011D0833) — reuse permitted, including for commercial purposes; attribution to the source requested.
