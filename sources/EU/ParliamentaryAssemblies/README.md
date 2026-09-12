# EU/ParliamentaryAssemblies — EU Joint / Regional Parliamentary Assemblies (Acts & Resolutions)

Acts and resolutions of the **EU's joint / regional parliamentary assemblies** —
the interparliamentary bodies the EU establishes with partner regions under its
international agreements. The corpus (~418 CELEX, 1997 onward) is dominated by the
**ACP-EU Joint Parliamentary Assembly** (JPA) and its predecessor the *ACP-EU
Joint Assembly* — the parliamentary institution of the ACP-EU partnership (Lomé →
Cotonou → the post-Cotonou "Samoa Agreement" OACPS-EU framework), bringing
together Members of the European Parliament and parliamentarians of the African,
Caribbean and Pacific (ACP / OACPS) group of states (~85% of the series) — and
also includes the **Euronest Parliamentary Assembly** (EU-Eastern Partnership),
the **EuroLat** Euro-Latin American Parliamentary Assembly and the
Union-for-the-Mediterranean assembly. They adopt resolutions on development
cooperation, human rights, trade, peace and security and other political
questions of common concern.

## Source

- Registry: EU Publications Office **CELLAR** (SPARQL + content negotiation)
- CELEX form: `2{YYYY}P{NNNN}` (sector 2, `P` = parliamentary-assembly act class)
- Series size: ~418 CELEX, 1997 onward
- Type: `doctrine` (non-binding parliamentary resolutions / soft law)

## How it works

1. Enumerate the whole `^2[0-9]{4}P[0-9]` series via the public CELLAR SPARQL
   endpoint (single paged query — the corpus is well under the 10K OFFSET
   ceiling).
2. Fetch full text via CELLAR HTTP content negotiation: OJ/Formex **xHTML** on
   the bare CELEX (modern acts), **OJ HTML** (bare CELEX `text/html` or the
   language-suffixed CELEX `.ENG`), and a born-digital **PDF** (PyMuPDF)
   fallback. CELEX that are metadata-only (no manifestation) are skipped.
3. Normalize to the standard schema.

This bypasses the `eur-lex.europa.eu` AWS-WAF that 202-challenges datacenter IPs,
so it is fleet-safe. Same CELLAR recipe as `EU/ESC-Opinions` and
`EU/InternationalAgreements`, narrowed to the sector-2 `P` descriptor.

Additive to existing EU sources: `EU/EUR-Lex` enumerates sector-3 legislation
only, and the sector-2 siblings `EU/InternationalAgreements` (`A`) and
`EU/JointBodyDecisions` (`D`) never cover the `P` parliamentary acts.

## Usage

```bash
python3 sources/EU/ParliamentaryAssemblies/bootstrap.py test              # probe SPARQL + one full text
python3 sources/EU/ParliamentaryAssemblies/bootstrap.py bootstrap --sample   # 15 sample records
python3 sources/EU/ParliamentaryAssemblies/bootstrap.py bootstrap-fast    # full corpus → data/records.jsonl
```

## License

[EU institutional reuse — Commission Decision 2011/833/EU](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32011D0833) — documents published by the EU Publications Office are reusable, including for commercial purposes, with attribution to the source. Commercial use permitted.
