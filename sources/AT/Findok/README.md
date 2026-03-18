# AT/Findok - Austrian Tax Doctrine (Finanzdokumentation)

## Overview

Findok (Finanzdokumentation) is the Austrian Federal Ministry of Finance's
comprehensive tax doctrine database. It contains:

- **Richtlinien** - Tax guidelines (EStR, UStR, KStR, etc.)
- **Amtliche Veröffentlichungen** - Official publications (BMF-AV)
- **Erlässe & Informationen** - Decrees and information
- **EAS** - Express Reply Service on international tax questions
- **BFG** - Federal Finance Court decisions
- **UFS** - Independent Tax Senate decisions (historical)

## API

This source uses the Findok JSON API discovered via SPA analysis:

- **Search/List**: `GET /findok/api/neuInFindok/sync?page=1&size=20&suchtypen=RICHTLINIEN`
- **Full Text**: `GET /findok/api/volltext?dokumentId=...&segmentId=...`
- **Guidelines**: `GET /findok/api/richtlinien/materieGruppe`
- **EAS List**: `GET /findok/api/easliste`

## Document Types (suchtypen)

| Type | Description |
|------|-------------|
| RICHTLINIEN | Guidelines |
| AMTLICHE_VEROEFFENTLICHUNGEN | Official publications |
| BFG | Federal Finance Court decisions |
| UFS | Independent Tax Senate decisions |
| EAS | Express Reply Service |
| ERLAESSE | Decrees |

## Usage

```bash
# Test API connectivity
python bootstrap.py test-api

# Fetch sample records for validation
python bootstrap.py bootstrap --sample

# Full bootstrap (all documents)
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## Data Coverage

- ~10,500 guidelines (RICHTLINIEN)
- Federal Finance Court decisions (BFG)
- Historical UFS decisions
- EAS international tax responses
- Official BMF publications

## Notes

- Full text is available via the `/api/volltext` endpoint
- Content is returned as HTML which is cleaned to plain text
- Documents are paginated at 100 per page
- Rate limiting: 0.5 requests/second

## Links

- [Findok Main Site](https://findok.bmf.gv.at/findok)
- [BMF Information Page](https://www.bmf.gv.at/public/informationen/findok.html)
- [data.gv.at Dataset](https://www.data.gv.at/katalog/dataset/finanzdokumentation-findok)
