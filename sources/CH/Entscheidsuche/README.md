# CH/Entscheidsuche - Swiss Court Decisions

Swiss court decisions from all federal and cantonal courts via [entscheidsuche.ch](https://entscheidsuche.ch).

## Coverage

- **Federal courts (CH)**: 294K+ decisions
  - Bundesgericht (Federal Supreme Court)
  - Bundesverwaltungsgericht (Federal Administrative Court)
  - Bundesstrafgericht (Federal Criminal Court)
  - Bundespatentgericht (Federal Patent Court)
- **All 26 cantons**: 539K+ decisions from cantonal courts
- **Total**: 833K+ court decisions

## API

Uses Elasticsearch API at `https://entscheidsuche.ch/_search.php`.

API documentation: https://entscheidsuche.ch/pdf/EntscheidsucheAPI.pdf

### Terms of Use

- Open API, free to use
- Be kind to the server (use rate limiting)
- Mention entscheidsuche.ch as source for commercial use

## Languages

- German (de)
- French (fr)
- Italian (it)
- Romansh (rm)

## Usage

```bash
# Fetch sample data
python bootstrap.py bootstrap --sample

# Get statistics
python bootstrap.py stats

# Fetch by canton
python bootstrap.py fetch-canton --canton ZH --limit 100

# Fetch recent decisions
python bootstrap.py fetch-recent --days 30 --limit 100
```

## Data Format

Each record includes:

- `_id`: Unique document identifier
- `title`: Decision title (multilingual)
- `text`: Full text of the decision (from PDF extraction)
- `date`: Decision date
- `canton`: Canton code (CH, ZH, GE, etc.)
- `url`: Link to original PDF
- `hierarchy`: Court hierarchy metadata
- `language`: Document language

## License

Public domain court decisions. API usage is open.
