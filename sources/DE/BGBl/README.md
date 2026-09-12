# DE/BGBl - German Federal Law (Gesetze im Internet)

## Overview

This data source fetches German federal legislation from [gesetze-im-internet.de](https://www.gesetze-im-internet.de), the official portal providing free access to virtually the entire body of current German federal law.

## Data Source

- **Provider**: Federal Ministry of Justice and Consumer Protection (BMJV)
- **Format**: XML (zipped)
- **Coverage**: ~6,000+ federal laws and regulations
- **Full Text**: Yes - complete consolidated text of all laws
- **Language**: German
- **License**: Public Domain ([§ 5 UrhG](https://www.gesetze-im-internet.de/urhg/__5.html))

## API Access

No authentication required. Data is freely available.

### Endpoints

- **Table of Contents**: `https://www.gesetze-im-internet.de/gii-toc.xml`
- **Individual Law**: `https://www.gesetze-im-internet.de/{identifier}/xml.zip`

Example:
- German Basic Law: `https://www.gesetze-im-internet.de/gg/xml.zip`
- Civil Code: `https://www.gesetze-im-internet.de/bgb/xml.zip`

## XML Structure

Each law XML contains:
- **Metadata**: Abbreviation (jurabk), title (langue), publication reference
- **Norms**: Individual articles/paragraphs with full text
- **Amendments**: History of changes

## Usage

```bash
# Test mode (fetch 3 documents)
python bootstrap.py

# Bootstrap sample (fetch 10 documents with full text)
python bootstrap.py bootstrap --sample
```

## Sample Output

```json
{
  "_id": "BJNR000010949",
  "_source": "DE/BGBl",
  "_type": "legislation",
  "title": "Grundgesetz für die Bundesrepublik Deutschland",
  "abbreviation": "GG",
  "text": "Der Parlamentarische Rat hat am 23. Mai 1949...",
  "date": "1949-05-23",
  "url": "https://www.gesetze-im-internet.de/gg/"
}
```

## Record granularity

Records are emitted **one per section** (§ / Artikel), not one per law:

| field | example |
|-------|---------|
| `_id` | `BJNR001950896BJNE018702377` (the norm's official `doknr`) |
| `abbreviation` | `BGB` |
| `section` / `section_number` | `§ 195` / `195` |
| `heading` | `Regelmäßige Verjährungsfrist` |
| `context` | `Buch 1 Allgemeiner Teil > Abschnitt 5 Verjährung > ...` |
| `url` | `https://www.gesetze-im-internet.de/bgb/__195.html` |

Each section's `text` begins with the law title, abbreviation and the
`gliederungskennzahl` breadcrumb, so "§ 195 BGB" resolves to exactly one
document (issue #1621 — the BGB used to be a single 2.5M-character record
and never reached the index). Laws with no numbered sections — short
regulations and annex-only instruments — are still emitted whole, with an
empty `section`.

## License

Public domain under German law — [§ 5 UrhG](https://www.gesetze-im-internet.de/urhg/__5.html) (official works / amtliche Werke).

## Notes

- The XML files contain consolidated (current) versions of laws
- Historical versions are not included (only current state)
- For BGBl gazette issues (historical publications), see recht.bund.de
