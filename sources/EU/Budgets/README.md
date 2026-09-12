# EU/Budgets — EU Budgetary Acts

Budgetary acts of the **European Union** catalogued by the EU Publications Office
(CELLAR / EUR-Lex) under CELEX **sector 3, document class `B`**
(`3{YYYY}B{NNNN}`).

The `B` descriptor covers the Union's budgetary documents published in the
Official Journal: the **statements of revenue and expenditure** of the EU
institutions, agencies and joint undertakings (one per body per financial year),
their **amending budgets**, and the **definitive adoption** of the general budget
of the European Union. Examples include the statements of revenue and expenditure
for the Fusion for Energy Joint Undertaking (F4E), the EU Agency for Fundamental
Rights (FRA), and the EU Agency for Law Enforcement Cooperation (Europol). The
corpus is ~4,325 acts.

## Why this is additive

EU/EUR-Lex enumerates sector 3 by the **binding** resource-types only
(Regulations / Directives / Decisions and their implementing/delegated variants),
so it never pulls budgetary acts. C-series acts under the `Y` descriptor
(`EU/OJC-Acts`) and Council resolutions under `G` (`EU/CouncilResolutions`) are
*different* sets of CELEX works — no `_id` collision — so `B` is not otherwise
captured.

## How it works

1. **Enumerate** every budgetary act via the public CELLAR SPARQL endpoint (CELEX
   matching `^3[0-9]{4}B[0-9]`). The corpus is ~4,325 rows, under the SPARQL
   OFFSET ceiling, so a single paged sweep suffices.
2. **Fetch full text** from CELLAR via HTTP content negotiation: OJ/Formex
   **xHTML** for modern acts, an **OJ HTML** manifestation via the
   language-suffixed CELEX (`/resource/celex/{CELEX}.ENG`) for older ones, and a
   born-digital **PDF** stream (PyMuPDF) for the remainder. CELLAR content
   negotiation bypasses the `eur-lex.europa.eu` AWS-WAF that 202-challenges
   datacenter IPs, so it is fleet-safe.
3. **Normalize** to the standard schema. `_type` is `doctrine` (official
   budgetary documents published in the OJ; not binding secondary legislation).

## Usage

```bash
python3 sources/EU/Budgets/bootstrap.py test               # probe SPARQL + one act
python3 sources/EU/Budgets/bootstrap.py bootstrap --sample # save 15 sample records
python3 sources/EU/Budgets/bootstrap.py bootstrap-fast      # full corpus → data/records.jsonl
```

## License

[EU institutional reuse — Commission Decision 2011/833/EU](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32011D0833) — reuse permitted, including for commercial purposes; attribution to the source requested.
