# BE/MoniteurBelge - Belgian Official Journal

## Overview

Data source for Belgian federal legislation from the Moniteur Belge / Belgisch Staatsblad (Belgian Official Journal).

**Status:** Complete (with full text)
**Data Type:** Legislation
**Auth:** None
**License:** CC Zero (CC 0)

## Coverage

- **Document types:** loi (laws), decret (decrees), ordonnance (Brussels ordinances), arrete (royal/ministerial decrees)
- **Languages:** French, Dutch, German
- **Date range:** September 22, 1998 onwards (full text available)
- **Approximate volume:** ~267 laws per year, plus decrees and other legislation

## Endpoints Used

### ELI Year Listing
```
GET https://www.ejustice.just.fgov.be/eli/{type}/{year}
```
Lists all documents of a given type published in a year.

### ELI Full Text (Consolidated)
```
GET https://www.ejustice.just.fgov.be/eli/{type}/{yyyy}/{mm}/{dd}/{numac}/justel
```
Returns the consolidated (current) HTML text of the legislation.

### ELI Full Text (Original)
```
GET https://www.ejustice.just.fgov.be/eli/{type}/{yyyy}/{mm}/{dd}/{numac}/moniteur
```
Returns the original published text (as it appeared in the Moniteur Belge).

## Usage

```bash
# Test connectivity
python bootstrap.py test

# Fetch sample records (10+)
python bootstrap.py bootstrap --sample

# Full bootstrap (all years, all types)
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## Output Schema

| Field | Type | Description |
|-------|------|-------------|
| `_id` | string | NUMAC identifier |
| `_source` | string | Always "BE/MoniteurBelge" |
| `_type` | string | Always "legislation" |
| `title` | string | Full document title |
| `text` | string | Full text of the legislation (HTML stripped) |
| `date` | string | Date of enactment (ISO 8601) |
| `url` | string | ELI URL to consolidated text |
| `numac` | string | Belgian NUMAC identifier |
| `document_type` | string | loi/decret/ordonnance/arrete |
| `year` | integer | Year of enactment |
| `language` | string | Document language (fr/nl/de) |
| `eli_uri` | string | Full ELI URI |

## Sample Data

After running `bootstrap --sample`, sample records are saved to:
- `sample/record_NNNN.json` - Individual records
- `sample/all_samples.json` - Combined file

## Notes

- Rate limit: 1 request every 2 seconds (0.5 req/s)
- The Vlaamse Codex API (Flemish regional legislation) could be added as a separate source
- German-language documents are included but less common
