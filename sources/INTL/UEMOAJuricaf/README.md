# INTL/UEMOAJuricaf — Court of Justice of the WAEMU/UEMOA (via Juricaf)

Full-text decisions of the **Cour de justice de l'UEMOA** (Court of Justice of
the West African Economic and Monetary Union / Union Économique et Monétaire
Ouest-Africaine), the judicial organ that rules on the interpretation and
application of UEMOA law across its **8 member states**: Benin, Burkina Faso,
Côte d'Ivoire, Guinea-Bissau, Mali, Niger, Senegal and Togo.

Decisions are published by **AHJUCAF** on [juricaf.org](https://juricaf.org)
with the complete judgment text.

## Data access

- **JSON API:** `https://juricaf.org/recherche/+/facet_pays:UEMOA?format=json&page=1`
  returns the full result set (~95 decisions) in a single page (`docs[]`).
- **Full text:** each decision page `https://juricaf.org/arret/{id}` carries the
  complete judgment in `div#textArret`; metadata in `<meta name="dc.*">`.

Court/date metadata is read from the decision page and falls back to the JSON
listing (`juridiction`, `date_arret`).

## Distinct from

- `INTL/UEMOA-Legislation` — UEMOA legal instruments (règlements, directives,
  décisions), not court decisions.
- `INTL/CEMACCourt` — the CEMAC court (Central Africa), a different union.

## Usage

```bash
python bootstrap.py test                 # connectivity + one decision
python bootstrap.py bootstrap --sample   # 15 sample records
python bootstrap.py bootstrap-fast       # full corpus -> data/records.jsonl
```

## License

[ODbL 1.0](https://opendatacommons.org/licenses/odbl/1-0/) — Open Database
License (Juricaf / AHJUCAF). Commercial use permitted; attribution required;
share-alike applies.
