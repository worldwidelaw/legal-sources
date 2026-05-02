# CL/TTA — Tribunales Tributarios y Aduaneros

**Tax & Customs Courts of Chile**

First-instance tax and customs tribunal decisions from all 18 regional TTA courts across Chile. Covers disputes with the SII (Internal Revenue Service) and Customs, including tax assessments, customs valuations, and real estate appraisals.

## Data

- **Type:** Case law (sentencias)
- **Records:** ~11,500 decisions
- **Coverage:** 2012–present
- **Language:** Spanish
- **Full text:** Yes, via ElasticSearch attachment extraction

## API

Uses the public ElasticSearch endpoint at `ojv.tta.cl/buscador/obtienedocumentosfiltroexpandidoes` (no authentication required). The `attachment.content` field provides pre-extracted full text from PDF decisions.

Metadata is also available via the REST API at `ojv.tta.cl/api/integracion/fallos`.

## License

[Public Domain](https://www.tta.cl/) — Official Chilean judicial decisions are public domain under Chilean law (Art. 9, Ley 20.285 on Access to Public Information).
