# ES/DGT - Spanish General Tax Directorate

## Source Information

- **Name**: Dirección General de Tributos (DGT)
- **Country**: Spain (ES)
- **Data Type**: Doctrine (binding tax rulings)
- **URL**: https://petete.tributos.hacienda.gob.es/consultas/
- **Coverage**: 1997-present
- **Volume**: ~68,000+ binding rulings

## Description

The DGT (Dirección General de Tributos) publishes binding tax rulings (consultas vinculantes) that provide official interpretations of Spanish tax law. These rulings are binding on the Tax Administration and serve as important precedents for tax planning and compliance.

The PETETE database is the official repository for all DGT doctrine, including both binding and non-binding consultations.

## Data Structure

Each ruling contains:

- **num_consulta**: Ruling number (e.g., V0001-25 for binding rulings)
- **fecha_salida**: Date issued
- **organo**: Issuing department within DGT
- **normativa**: Relevant legal provisions
- **descripcion_hechos**: Description of facts
- **cuestion_planteada**: Question posed by the taxpayer
- **contestacion_completa**: Complete answer (full text)

## Usage

```bash
# Test connectivity
python bootstrap.py test

# Fetch sample records
python bootstrap.py bootstrap --sample

# Full bootstrap (warning: ~68,000+ documents)
python bootstrap.py bootstrap

# Update since last run
python bootstrap.py update
```

## Technical Notes

- The PETETE system requires browser-like headers for authentication
- SSL certificate verification is disabled due to certificate issues
- Rate limiting: 0.5 requests/second to be respectful
- Fetches by date ranges to handle large dataset
- Session must be initialized by visiting main page first

## License

Open government data under [Spanish Reuse of Public Sector Information regulations](https://datos.gob.es/en/terms).
