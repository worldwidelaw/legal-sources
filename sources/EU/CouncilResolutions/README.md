# EU/CouncilResolutions — Council Resolutions

Resolutions and declarations of the **Council of the European Union**, catalogued
by the EU Publications Office (CELLAR / EUR-Lex) under CELEX **sector 3, document
class `G`** (`3{YYYY}G{NNNN}`).

Council resolutions are **non-binding** acts (Art. 288 TFEU) by which the Council
— often together with the Representatives of the Governments of the Member States
— expresses a political commitment or invites action in an area of Union policy,
e.g. the Council Resolution on the EU Work Plan for Culture, on the European
Education Area, on a joint European degree label, and on the strategic framework
for European cooperation in education and training. The corpus spans 1961 to the
present (~157 acts).

## Why this is additive

EU/EUR-Lex enumerates sector 3 by the **binding** resource-types only
(Regulations / Directives / Decisions and their implementing/delegated variants),
so it never pulls Council resolutions. C-series resolutions carried under the `Y`
descriptor (`EU/OJC-Acts`) are a *different* set of CELEX works — no `_id`
collision — so `G` is not otherwise captured.

## How it works

1. **Enumerate** every resolution via the public CELLAR SPARQL endpoint (CELEX
   matching `^3[0-9]{4}G[0-9]`). The corpus is ~157 rows, far under the SPARQL
   OFFSET ceiling, so a single paged sweep suffices.
2. **Fetch full text** from CELLAR via HTTP content negotiation: OJ/Formex
   **xHTML** for modern resolutions, an **OJ HTML** manifestation via the
   language-suffixed CELEX (`/resource/celex/{CELEX}.ENG`) for older ones, and a
   born-digital **PDF** stream (PyMuPDF) for the remainder. CELLAR content
   negotiation bypasses the `eur-lex.europa.eu` AWS-WAF that 202-challenges
   datacenter IPs, so it is fleet-safe.
3. **Normalize** to the standard schema. `_type` is `doctrine` (non-binding soft
   law).

## Usage

```bash
python3 sources/EU/CouncilResolutions/bootstrap.py test               # probe SPARQL + one act
python3 sources/EU/CouncilResolutions/bootstrap.py bootstrap --sample # save 15 sample records
python3 sources/EU/CouncilResolutions/bootstrap.py bootstrap-fast      # full corpus → data/records.jsonl
```

## License

[EU institutional reuse — Commission Decision 2011/833/EU](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32011D0833) — reuse permitted, including for commercial purposes; attribution to the source requested.
