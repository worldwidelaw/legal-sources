# ES/Madrid - Boletín Oficial de la Comunidad de Madrid (BOCM)

## Overview
Official gazette of the Community of Madrid containing regional legislation, regulations, decrees, orders, resolutions, and official announcements.

## Data Source
- **URL**: https://www.bocm.es
- **Type**: legislation
- **Language**: Spanish
- **License**: Open Data (see [License](#license) below)
- **Auth**: None required

## API Access
BOCM provides structured XML documents at predictable URLs:

### Endpoints
- **Bulletins RSS**: `https://www.bocm.es/boletines.rss` - List of last 20 bulletins
- **Latest Bulletin RSS**: `https://www.bocm.es/ultimo-boletin.xml` - Orders from latest bulletin
- **Summaries RSS**: `https://www.bocm.es/sumarios.rss` - Links to recent summaries
- **Document XML**: `https://www.bocm.es/boletin/CM_Orden_BOCM/{year}/{month}/{day}/BOCM-{date}-{order}.xml`

### Document Formats
Each document is available in multiple formats:
- XML (structured with metadata and full text)
- PDF
- EPUB
- JSON-LD

### XML Structure
```xml
<documento>
  <metadatos>
    <identificador>BOCM-20260318-1</identificador>
    <origen_legislativo>Comunidad de Madrid</origen_legislativo>
    <departamento>CONSEJERÍA DE...</departamento>
    <rango>RESOLUCIÓN</rango>
    <fecha_publicacion>2026/03/18</fecha_publicacion>
    <titulo>...</titulo>
    ...
  </metadatos>
  <analisis>
    <seccion>I. COMUNIDAD DE MADRID</seccion>
    <apartado>B) Autoridades y Personal</apartado>
    <organismo>...</organismo>
    <tipo_disposicion>...</tipo_disposicion>
  </analisis>
  <texto>Full text content here...</texto>
</documento>
```

## Usage

```bash
# Quick connectivity test
python bootstrap.py test

# Fetch sample data (10+ records)
python bootstrap.py bootstrap --sample

# Full bootstrap (last year of data)
python bootstrap.py bootstrap

# Incremental updates
python bootstrap.py update
```

## Data Coverage
- Historical data available from 1983
- Published Monday through Saturday (excluding Sundays and holidays)
- Multiple documents per bulletin (typically 10-100+ per day)
- Document types include: Leyes, Decretos, Órdenes, Resoluciones, Correcciones, etc.

## License

Open government data under [Spanish Reuse of Public Sector Information regulations](https://datos.gob.es/en/terms). Free public access per Decree 2/2010.

## Notes
- Full text is always available via XML endpoint
- Rate limiting: 2 requests per second recommended
- No authentication required
