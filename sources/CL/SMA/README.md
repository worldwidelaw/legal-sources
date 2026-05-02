# CL/SMA - Superintendencia del Medio Ambiente

Environmental enforcement sanction procedures from Chile's SMA, published
via SNIFA (Sistema Nacional de Información de Fiscalización Ambiental).

## Data

- **Type:** case_law (sanction procedure resolutions)
- **Volume:** ~4,000+ sanction procedures (2013-present)
- **Language:** Spanish
- **Coverage:** Environmental violations including RCA breaches, emission
  standard violations, and environmental management plan non-compliance

## Source

- **Portal:** https://snifa.sma.gob.cl/Sancionatorio
- **Detail pages:** `/Sancionatorio/Ficha/{id}` for each procedure
- **Documents:** Resolution PDFs via `/General/Descargar/{doc_id}`

## Strategy

1. Iterate through Ficha IDs (1 to ~4500)
2. Parse HTML for case metadata (expedition number, entity, unit, infractions)
3. Identify the resolution sancionatoria PDF
4. Download and extract full text via `common/pdf_extract`

## License

[Public Domain (Chilean Government)](https://www.leychile.cl/navegar?idNorma=276363) — official enforcement decisions published under Chile's Transparency Law (Ley 20.285).
