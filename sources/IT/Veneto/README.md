# IT/Veneto -- Veneto Regional Legislation

Fetches regional legislation from the Bollettino Ufficiale della Regione del Veneto (BUR).

## Data Source

- **Portal**: http://bur.regione.veneto.it/BurvServices/
- **Coverage**: 2004-present
- **Document Types**: Regional laws (Leggi Regionali), regulations, decrees, deliberations
- **Volume**: ~4,400+ regional laws

## Strategy

1. Search the BUR database by act type (tipoAtto=11 for Leggi Regionali)
2. Extract document IDs from search result links
3. Fetch individual law detail pages
4. Parse full text from HTML content

## Endpoints

- **Search**: `http://bur.regione.veneto.it/BurvServices/Pubblica/SommarioRicerca.aspx`
  - Parameters: `tipoRicerca=base`, `oggetto=*`, `daDta=DD/MM/YYYY`, `aDta=DD/MM/YYYY`, `tipoAtto=11`

- **Detail**: `http://bur.regione.veneto.it/BurvServices/Pubblica/DettaglioLegge.aspx?id={id}`

## Document Types (tipoAtto codes)

- 11: Leggi Regionali (Regional Laws)
- 7: Decreti (Decrees)
- 9: Deliberazioni della Giunta Regionale (Regional Council Deliberations)
- 15: Sentenze ed Ordinanze (Judgments and Orders)
- 6: Concorsi (Public competitions)
- 1: Appalti (Public contracts)
- 3: Avvisi (Notices)

## Usage

```bash
# Quick connectivity test
python bootstrap.py test

# Fetch sample records (10+)
python bootstrap.py bootstrap --sample

# Full bootstrap
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## License

Italian public sector information - CC0 1.0 Universal (Public Domain Dedication)
