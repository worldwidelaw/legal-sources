# IT/EmiliaRomagna — Emilia-Romagna Regional Legislation

Fetches regional legislation from Emilia-Romagna via the Demetra database.

## Source Information

- **Portal**: [Demetra - Normativa, atti e sedute della Regione Emilia-Romagna](https://demetra.regione.emilia-romagna.it/al/)
- **Download Page**: [Scarica i documenti](https://demetra.regione.emilia-romagna.it/al/link-utili/scarica-i-dati/)
- **Data Format**: NIR XML (Norme in Rete), same format as national Normattiva
- **Coverage**: 1971-present
- **License**: CC BY 4.0

## Document Types

| Code | Type | Description |
|------|------|-------------|
| LR | Legge Regionale | Regional laws |
| RR | Regolamento Regionale | Regional regulations |
| RI | Regolamento Interno | Internal regulations of the Assembly |

## Legislatures

| # | Years | Notes |
|---|-------|-------|
| I | 1970-1975 | First regional legislature |
| II | 1975-1980 | |
| III | 1980-1985 | |
| IV | 1985-1990 | |
| V | 1990-1995 | |
| VI | 1995-2000 | |
| VII | 2000-2005 | |
| VIII | 2005-2010 | |
| IX | 2010-2015 | |
| X | 2015-2020 | |
| XI | 2020-2025 | |
| XII | 2025-2030 | Current |

## Usage

```bash
# Test connection
python bootstrap.py test

# Fetch sample records for validation
python bootstrap.py bootstrap --sample

# Full bootstrap (all legislatures)
python bootstrap.py bootstrap

# Update (recent legislatures only)
python bootstrap.py update
```

## Data Structure

ZIP files are named: `{legislature}_{type}_{format}.zip`

Example: `12_LR_xml.zip` = Legislature XII, Regional Laws, XML format

Inside the ZIP:
- XML files: `{legislature}_{type}_{year}_{number}.xml`
- PDF attachments: `{legislature}_{type}_{year}_{number}_A{n}.pdf`

## XML Format

Documents use the NIR (Norme in Rete) XML schema, same as national legislation:

```xml
<NIR xmlns:dsp="http://www.normeinrete.it/nir/disposizioni/1.0">
  <Legge>
    <meta>
      <descrittori>
        <urn>urn:nir:regione.emilia.romagna:legge:2025-03-31;1</urn>
      </descrittori>
    </meta>
    <intestazione>
      <tipoDoc>LEGGE REGIONALE</tipoDoc>
      <dataDoc norm="20250331">31 marzo 2025</dataDoc>
      <numDoc>1</numDoc>
      <titoloDoc>DISPOSIZIONI IN MATERIA TRIBUTARIA</titoloDoc>
    </intestazione>
    <articolato>
      <articolo id="art1">...</articolo>
    </articolato>
  </Legge>
</NIR>
```

## Notes

- Update frequency: Weekly
- All attachments are downloaded as PDF regardless of requested format
- The same XML parsing logic is used as IT/Lombardia (both use NIR format)
