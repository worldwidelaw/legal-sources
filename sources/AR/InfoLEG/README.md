# AR/InfoLEG - Sistema de Información Legislativa

Official Argentine legislation database from the Ministry of Justice.

## Data Source

- **URL**: https://datos.jus.gob.ar/dataset/base-de-datos-legislativos-infoleg
- **License**: CC BY 4.0
- **Update Frequency**: Monthly
- **Language**: Spanish

## Coverage

- Laws, decrees, administrative decisions, resolutions, dispositions
- All normative acts published in the Official Bulletin since May 1997
- Referenced legislation dating back to 1853
- National scope (federal legislation)

## Data Access

The data is accessed via:
1. **Bulk CSV/ZIP download** from datos.jus.gob.ar (CKAN portal)
2. **Full text HTML pages** from servicios.infoleg.gob.ar

The CSV provides metadata including URLs to full text documents.

## Usage

```bash
# Test API connectivity
python bootstrap.py test-api

# Fetch sample records (15 documents with full text)
python bootstrap.py bootstrap --sample

# Full bootstrap (fetch all documents)
python bootstrap.py bootstrap
```

## Schema

Key fields:
- `id_norma`: Unique identifier
- `tipo_norma`: Type (Ley, Decreto, Resolución, etc.)
- `numero_norma`: Norm number
- `title`: Brief title
- `text`: Full text content
- `date`: Enactment date
- `organismo_origen`: Issuing authority
- `url`: Link to original on InfoLEG

## Notes

- ~37% of records in the sample have direct full text URLs
- Full text pages are simple HTML with Latin-1 encoding
- Rate limiting: 1 request/second to avoid overloading the server
