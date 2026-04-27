# BR/TST — Tribunal Superior do Trabalho (Brazilian Labor Supreme Court)

## Data Source

The TST jurisprudence backend API at `jurisprudencia-backend2.tst.jus.br` provides
access to 8M+ court decisions (acórdãos) from Brazil's Supreme Labor Court.

## API Details

- **Search endpoint**: `POST /rest/pesquisa-textual/{start}/{size}`
- **Auth**: None required (open data)
- **Format**: JSON response with HTML full text in `inteiroTeorHtml` field
- **Pagination**: 1-based start index, configurable page size

### Request body (all fields optional)

```json
{
  "publicacaoInicial": "2026-01-01",
  "publicacaoFinal": "2026-12-31",
  "julgamentoInicial": "2026-01-01",
  "julgamentoFinal": "2026-12-31",
  "orgaosJudicantes": [],
  "ministros": [],
  "classesProcessuais": [],
  "assuntos": []
}
```

### Auxiliary endpoints (GET)

- `/rest/ministros` — list of justices
- `/rest/orgaos-judicantes` — judging bodies (Turmas, Seções)
- `/rest/classes-processuais` — procedural classes
- `/rest/assuntos` — subject matter taxonomy
- `/rest/indicadores` — indicators

## Data Coverage

- **Type**: Case law (acórdãos, despachos, etc.)
- **Volume**: ~8.3 million records
- **Content**: Full text of decisions (inteiro teor) + ementa + dispositivo
- **Language**: Portuguese (pt-BR)

## Usage

```bash
python bootstrap.py test               # Connectivity test
python bootstrap.py bootstrap --sample # Fetch 15 sample records
python bootstrap.py bootstrap          # Full bootstrap (all records)
```

## License

[Open Government Data](https://jurisprudencia.tst.jus.br/) — official decisions published by the Superior Labor Court (TST) of Brazil.
