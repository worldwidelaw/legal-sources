# PE/INDECOPI — Peru INDECOPI Tribunal Resolutions

## Source
INDECOPI (Instituto Nacional de Defensa de la Competencia y de la Protección
de la Propiedad Intelectual) institutional DSpace 7 repository.

**URL:** https://repositorio.indecopi.gob.pe/

## Data Access
Uses the DSpace 7 REST API (HAL+JSON, no authentication required).

- Search/browse items: `/backend/api/discover/search/objects`
- Item bundles: `/backend/api/core/items/{uuid}/bundles`
- Bitstream content: `/backend/api/core/bitstreams/{uuid}/content`

## Coverage
~643 items across 14 legal communities:
- Resoluciones (curated highlights)
- Sala Defensa de la Competencia
- Sala Propiedad Intelectual
- Sala Protección al Consumidor
- Competencia desleal
- Libre competencia
- Eliminación de barreras burocráticas
- Signos Distintivos (trademarks)
- Derecho de Autor (copyright)
- Invenciones y Nuevas Tecnologías (patents)
- Procedimientos concursales (bankruptcy)
- Dumping y subsidios

## Full Text
1. Pre-extracted text from DSpace TEXT bundle (preferred)
2. PDF extraction via PyMuPDF fallback for scanned documents

## Notes
- The main search portal (servicio.indecopi.gob.pe/buscadorResoluciones/) has
  SSL/connectivity issues and uses JSF/Seam (no REST API), so we use the
  DSpace repository instead.
- Some older PDFs are scanned images with no extractable text — these are skipped.

## License

[Open Government Data](https://repositorio.indecopi.gob.pe/) — official tribunal resolutions published by INDECOPI, Peru's competition and IP authority.
