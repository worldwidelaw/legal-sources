# MX/PoderJudicialTabasco — Tribunal Superior de Justicia del Estado de Tabasco

Public-version court decisions (*versiones públicas de sentencias*) from the
Tribunal Superior de Justicia del Estado de Tabasco, Mexico.

## Coverage

- **Segunda instancia (appellate / fallos):** Tocas and Amparos in Civil and
  Penal matters decided by the Salas of the Tribunal Superior. Full appellate
  judgments with legal reasoning.
- **Primera instancia (trial courts):** sentencias from the juzgados of all
  municipios of Tabasco.

## Access

Public portal: <https://tsj-tabasco.gob.mx/sentencias-version-publica/>

The portal is driven by an AJAX endpoint (`/resources/php/ajax.php`) that returns
HTML result tables, each row linking to a full-text PDF:

- `funcion=ObtenListaSentenciasPublicas2daInsta` — appellate, filtered by
  `TipoBusqueda` (T=Tocas, A=Amparos) × `Materia` (C=Civil, P=Penal). An empty
  Toca/Fecha filter returns the full catalogue per combination.
- `funcion=ObtenListaSentenciasPublicas1raInsta` — trial court, filtered by
  `Municipio` and `Juzgado`. The Juzgado list per Municipio is loaded from
  `funciones.php?funcion=juzgado2&Id={municipio}`.

The decision body is the per-row PDF (`/resources/pdf/transparencia2/...`),
downloaded and text-extracted via `common.pdf_extract`.

No authentication is required. The site WAF/geo-blocks some non-Mexican IPs; if
discovery returns empty rows, run from a Mexican region/proxy.

## Usage

```bash
python bootstrap.py test                # connectivity + sample PDF check
python bootstrap.py bootstrap --sample  # 15 sample records
python bootstrap.py bootstrap-fast --full
```

## License

[Open government data](https://datos.gob.mx) — public-version judicial decisions
published by a Mexican state court for transparency purposes. Commercial use
permitted.
