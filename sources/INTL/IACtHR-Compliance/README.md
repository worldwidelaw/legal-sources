# INTL/IACtHR-Compliance — Inter-American Court of Human Rights: Monitoring Compliance with Judgment

Compliance-monitoring resolutions (**Resoluciones de Supervisión de Cumplimiento
de Sentencia**) of the **Inter-American Court of Human Rights** (Corte
Interamericana de Derechos Humanos, "Corte IDH"), the autonomous judicial organ
of the Organization of American States (OAS), seated in San José, Costa Rica.

After the Court delivers a contentious judgment, it retains jurisdiction to
supervise the judgment's execution. It issues periodic **binding** resolutions
assessing whether the responsible state has complied with the reparations it
ordered, keeping each case open until compliance is complete. This
compliance-supervision jurisprudence is a distinct, authoritative body of case
law that develops the Court's standards on reparations and state obligations.

## Coverage

- **~841 resolutions**, from the mid-1990s to the present.
- Both **active** (cases still under supervision) and **archived** (cases closed
  for full compliance) dockets.
- Includes aggregate multi-case resolutions (e.g. resolutions covering several
  Guatemalan cases at once).
- Full text in **Spanish**, extracted from **born-digital PDFs** (text layer, no
  OCR required).

## Distinct from the sibling Corte IDH sources

| Source | Content | Series |
|--------|---------|--------|
| `INTL/IACtHR` | Contentious judgments | Series C |
| `INTL/IACtHR-Advisory` | Advisory opinions | Series A |
| `INTL/IACtHR-ProvisionalMeasures` | Provisional measures | Medidas Provisionales |
| **`INTL/IACtHR-Compliance`** | **Compliance-monitoring resolutions** | **Supervisión de Cumplimiento** |

## Method

The two "casos en supervisión por país" listing pages
(`casos_en_supervision_por_pais.cfm` and its `_archivados` counterpart) render
each supervised case as a table row: case name, judgment date, and the
individual compliance resolutions. Each resolution is a dated link to a
born-digital PDF under `/docs/supervisiones/`.

The scraper keeps only anchors whose visible text is a Spanish date (the real
resolutions), excluding the undated `{case}c.pdf` / `{case}p.pdf` status-summary
documents and the separate party-brief (escritos) pages. Each PDF is downloaded
and its full text extracted; the issue date comes from the anchor text (with the
PDF header as a fallback), and the case name from the row.

## Usage

```bash
python bootstrap.py test               # Connectivity + count
python bootstrap.py bootstrap --sample # Fetch sample records
python bootstrap.py bootstrap          # Full pull
python bootstrap.py bootstrap-fast     # Full pull (fleet alias)
python bootstrap.py update             # Incremental
```

## License

> ⚠️ **Commercial use restricted.** See terms below.

[CC BY-NC-ND 3.0](https://creativecommons.org/licenses/by-nc-nd/3.0/) — matches
the sibling `INTL/IACtHR` sources for Corte IDH jurisprudence. Attribution
required; non-commercial; no derivatives. The resolutions are official public
edicts of the Inter-American Court published on its website.
