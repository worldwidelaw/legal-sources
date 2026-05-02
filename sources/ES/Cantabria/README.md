# ES/Cantabria — Cantabria Regional Legislation (BOC)

Fetches legislation from the Boletín Oficial de Cantabria (BOC), the official
gazette of the Autonomous Community of Cantabria in northern Spain.

## Data source

- **URL**: https://boc.cantabria.es/boces/
- **Coverage**: 1999–present (digital archive)
- **Section**: 1. Disposiciones Generales (General Provisions)
- **Document types**: Laws, Decrees, Orders, Resolutions, Ordinances
- **Language**: Spanish (es)

## Strategy

1. JSON API (`busquedaBoletines.do?mes=M&year=Y`) lists bulletins per month.
2. HTML bulletin index (`verBoletin.do?idBolOrd=ID`) parsed for Section 1 entries.
3. Individual PDFs downloaded (`verAnuncioAction.do?idAnuBlob=ID`) and text
   extracted via pdfplumber.

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — Gobierno de Cantabria open data. Attribution required.
