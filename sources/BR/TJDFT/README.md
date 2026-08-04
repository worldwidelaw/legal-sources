# BR/TJDFT — Tribunal de Justiça do Distrito Federal e dos Territórios

Brazilian capital district court (Brasília / Federal District).

## Why this source is notable

TJDFT is the **most open court portal among all 27 Brazilian TJ state courts**.
While 20+ other state courts use the ESAJ portal (which enforces a hard 250-result
cap per query with no pagination), TJDFT uses a public **Elasticsearch REST API**
that supports unrestricted full-text keyword search with complete pagination.

This makes TJDFT particularly valuable for systematic legal research:
- No authentication required
- No result cap (tested with queries returning 2,000+ results)
- Full decision text (ementa + decisao) in every response
- Structured metadata: process number, relator, orgão julgador, dates

## API

**Endpoint:** `POST https://jurisdf.tjdft.jus.br/api/v1/pesquisa`

**Request body:**
```json
{
  "query": "agua saneamento",
  "pagina": 0,
  "tamanho": 20,
  "espelho": false,
  "sinonimos": false
}
```

**Response fields:**
| Field | Description |
|-------|-------------|
| `hits.value` | Total matching decisions |
| `registros[].uuid` | Unique decision identifier |
| `registros[].processo` | Process number |
| `registros[].dataJulgamento` | Judgment date (ISO 8601) |
| `registros[].dataPublicacao` | Publication date |
| `registros[].nomeRelator` | Reporting justice name |
| `registros[].descricaoOrgaoJulgador` | Deciding panel |
| `registros[].marcadores.ementa[]` | Headnote (may contain `<mark>` tags) |
| `registros[].decisao` | Full decision text |
| `registros[].base` | Database (ACORDAOS, DECISOES, etc.) |

## Usage

```bash
python bootstrap.py bootstrap          # Full collection (all keywords)
python bootstrap.py bootstrap --sample # 15 sample records
python bootstrap.py test               # API connectivity test
python bootstrap.py update             # Incremental (since last run)
```

## Coverage

- **8,400+** decisions collected in water/sanitation law domain (2016–2026)
- Covers all TJDFT judging panels
- Date range: no enforced limit; records go back to early 2000s
- Language: Portuguese (pt-BR)

## Rate limiting

The API is generous. A 0.3s delay between requests is conservative and safe.
Do not reduce below 0.1s — respect the public server.

## Data source

Discovered and validated as part of the
[Global Water Law Judicial Decisions Dataset](https://github.com/jrklaus8/water-law-dataset)
(DOI: [10.5281/zenodo.19836413](https://doi.org/10.5281/zenodo.19836413)).
