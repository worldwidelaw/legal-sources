# BR/STJDadosAbertos

Brazilian Superior Court of Justice (STJ) open data.

## Source

CKAN portal: [dadosabertos.web.stj.jus.br](https://dadosabertos.web.stj.jus.br/dataset/)

## Data

Uses "espelhos de acórdãos" JSON datasets from 10 judging bodies:
- Corte Especial, 3 Seções (1ª, 2ª, 3ª), 6 Turmas (1ª-6ª)

Each record contains:
- ementa: legal headnote/summary
- decisao: full decision text
- ministroRelator: reporting justice
- dataDecisao/dataPublicacao: dates
- referenciasLegislativas: cited legislation

## Access

No authentication required. CKAN API provides JSON snapshots per judging body.

## Usage

```bash
python bootstrap.py test               # Test connectivity
python bootstrap.py bootstrap --sample # Fetch 15 samples
python bootstrap.py bootstrap          # Full fetch (all bodies, latest snapshots)
```
