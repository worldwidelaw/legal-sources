# EU/RulesOfProcedure — Rules of Procedure & Institutional Acts

The EU's **institutional and procedural** acts, catalogued by the EU Publications
Office (CELLAR / EUR-Lex) under CELEX **sector 3, document class `Q`**
(`3{YYYY}Q{NNNN}`).

The `Q` descriptor gathers the **rules of procedure** of the EU institutions,
courts and bodies — the Court of Justice, the General Court, the European Economic
and Social Committee, the Committee of the Regions and the agencies' management
boards — together with their amendments, the Union courts' **practice directions**
and **practice rules**, **inter-institutional framework agreements** (e.g. the
Framework Agreement on relations between the European Parliament and the
Commission), **codes of conduct**, and the internal decisions of bodies such as the
European Data Protection Supervisor on records/archives management. The corpus
spans the 1990s to the present (~526 acts).

## Why this is additive

EU/EUR-Lex enumerates sector 3 by the ordinary **binding** resource-types only
(Regulations / Directives / Decisions and their implementing/delegated variants).
The `Q` institutional/procedural acts carry the dedicated rules-of-procedure /
internal-agreement resource types (not `DEC`), so EU/EUR-Lex never pulls them. No
`_id` collision with any other descriptor.

## How it works

1. **Enumerate** every act via the public CELLAR SPARQL endpoint (CELEX matching
   `^3[0-9]{4}Q[0-9]`). The corpus is ~526 rows, far under the SPARQL OFFSET
   ceiling, so a single paged sweep suffices.
2. **Fetch full text** from CELLAR via HTTP content negotiation: OJ/Formex
   **xHTML** for modern acts, an **OJ HTML** manifestation via the
   language-suffixed CELEX (`/resource/celex/{CELEX}.ENG`) for older ones, and a
   born-digital **PDF** stream (PyMuPDF) for the remainder. CELLAR content
   negotiation bypasses the `eur-lex.europa.eu` AWS-WAF that 202-challenges
   datacenter IPs, so it is fleet-safe. Some `Q` CELEX carry a parenthesised
   sub-part (e.g. `32020Q1012(01)`); the CELEX is URL-encoded before content
   negotiation.
3. **Normalize** to the standard schema. `_type` is `doctrine`
   (institutional/procedural acts).

## Usage

```bash
python3 sources/EU/RulesOfProcedure/bootstrap.py test               # probe SPARQL + one act
python3 sources/EU/RulesOfProcedure/bootstrap.py bootstrap --sample # save 15 sample records
python3 sources/EU/RulesOfProcedure/bootstrap.py bootstrap-fast      # full corpus → data/records.jsonl
```

## License

[EU institutional reuse — Commission Decision 2011/833/EU](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32011D0833) — reuse permitted, including for commercial purposes; attribution to the source requested.
