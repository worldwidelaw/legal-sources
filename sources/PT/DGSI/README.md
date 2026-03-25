# PT/DGSI — Portuguese Courts of Appeal & Other DGSI Databases

Fetches case law from DGSI sub-databases not covered by PT/STA or PT/SupremeCourt.

## Databases covered

| Code | Court | Decisions |
|------|-------|-----------|
| jtrp | Tribunal da Relação do Porto | ~63,000 |
| jtrl | Tribunal da Relação de Lisboa | ~60,000 |
| jtrc | Tribunal da Relação de Coimbra | ~16,000 |
| jtrg | Tribunal da Relação de Guimarães | ~15,000 |
| jtre | Tribunal da Relação de Évora | ~18,000 |
| jtca | Tribunal Central Administrativo Sul | ~30,000 |
| jtcn | Tribunal Central Administrativo Norte | ~20,000 |
| jcon | Tribunal dos Conflitos | ~1,200 |
| cajp | Julgados de Paz | ~7,300 |

## Access method

- JSON enumeration: `/{db}.nsf/Por+Ano?ReadViewEntries&Start=N&Count=N&OutputFormat=JSON`
- Full document: `/{db}.nsf/0/{unid}?OpenDocument&ExpandSection=1`
- No auth required, no anti-bot protection
- Lotus Notes/Domino backend, ISO-8859-1 encoding
