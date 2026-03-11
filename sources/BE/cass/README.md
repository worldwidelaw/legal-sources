# BE/CASS - Belgian Court of Cassation

Belgian Supreme Court case law from JUPORTAL.

## Source Details

- **Country**: Belgium
- **Court**: Cour de cassation / Hof van Cassatie
- **Type**: Case law
- **URL**: https://juportal.be
- **Records**: ~49,000+ decisions (since 1958)
- **Languages**: French, Dutch, German
- **License**: Open Government Data

## Data Access Strategy

### Document Discovery
Uses EU ECLI sitemaps published by JUPORTAL:
- Sitemap index URLs listed in robots.txt
- Daily sitemap updates with ECLI metadata
- Rich metadata: date, abstract, subject, keywords
- Filter for court code "CASS"

### Full Text Retrieval
Content endpoint provides full decision text:
```
https://juportal.be/content/ECLI:BE:CASS:2023:ARR.20231215.1F.2
```

## ECLI Format

Belgian Court of Cassation ECLIs follow this pattern:
```
ECLI:BE:CASS:{year}:{type}.{date}.{chamber}.{number}
```

- **CASS**: Court of Cassation code
- **type**: ARR (arrêt/arrest), CONC (conclusion/conclusie)
- **chamber**: 1F, 2N, 3F, etc. (F=French, N=Dutch)

## Usage

```bash
# Test connectivity
python bootstrap.py test

# Fetch sample records (12 documents)
python bootstrap.py bootstrap --sample

# Full bootstrap
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## Schema

| Field | Type | Description |
|-------|------|-------------|
| ecli | string | ECLI identifier |
| text | string | Full decision text |
| date | date | Decision date |
| court | string | Court name |
| chamber | string | Court chamber |
| language | string | Decision language (fr/nl/de) |
| title | string | Case title/parties |
| abstract | string | Summary/headnotes |
| subjects | array | Legal domains |
| keywords | array | Thesaurus keywords |
| role_number | string | Case role number |

## Rate Limits

- 1 request per second
- Uses JUPORTAL sitemap infrastructure

## Notes

- Part of Belgian judiciary's open data initiative
- Sitemaps are published for EU ECLI search engine indexing
- Full text available in original language only
