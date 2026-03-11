# DE/BGBl - German Federal Law (Gesetze im Internet)

## Overview

This data source fetches German federal legislation from [gesetze-im-internet.de](https://www.gesetze-im-internet.de), the official portal providing free access to virtually the entire body of current German federal law.

## Data Source

- **Provider**: Federal Ministry of Justice and Consumer Protection (BMJV)
- **Format**: XML (zipped)
- **Coverage**: ~6,000+ federal laws and regulations
- **Full Text**: Yes - complete consolidated text of all laws
- **Language**: German
- **License**: Public Domain (Amtliche Werke, § 5 UrhG)

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

## Notes

- The XML files contain consolidated (current) versions of laws
- Historical versions are not included (only current state)
- For BGBl gazette issues (historical publications), see recht.bund.de
