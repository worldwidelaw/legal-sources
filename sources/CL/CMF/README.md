# CL/CMF — Comisión para el Mercado Financiero (Chile)

Chile's integrated financial market regulator, successor to the SVS
(Superintendencia de Valores y Seguros). Publishes binding financial
regulations and enforcement decisions.

## Data Coverage

| Type | Description | Count | Period |
|------|-------------|-------|--------|
| NCG | Normas de Carácter General | ~500 | 1981–present |
| CIR | Circulares | ~50 | 1981–present |
| OFC | Oficios Circulares | ~50 | 1981–present |
| Sanciones | Enforcement resolutions | ~1,450 | 2002–present |

## How It Works

- **Normativa**: GET search to `normativa2.php` by year, parse HTML table,
  download PDFs via `ver_sgd.php`
- **Sanctions**: Scrape listing at `sanciones_mercados_entidad.php`,
  download PDFs via `ver_sgd.php`
- Full text extracted from PDFs using `common/pdf_extract`

## License

[Chilean Government Public Domain (Ley 20.285)](https://www.leychile.cl/navegar?idNorma=276363) — official regulatory documents published by a Chilean government agency under the Transparency Law.
