# LV/LegislativeDatabase — Latvian Legislation (Likumi.lv)

**Country:** Latvia (LV)
**URL:** https://likumi.lv
**Data Type:** Legislation
**Auth:** None (public access)
**License:** Open government data (Latvijas Vēstnesis)

## Overview

Likumi.lv is Latvia's official legislation database providing free access to systematized (consolidated) legal acts of the Republic of Latvia. The portal is maintained by Latvijas Vēstnesis, the official publisher.

## Data Coverage

- **Documents:** ~49,000 legal acts
- **Period:** 1999 to present
- **Language:** Latvian

### Document Types

| Type | Latvian | Description |
|------|---------|-------------|
| Likums | Law | Parliamentary laws |
| Noteikumi | Regulation | Cabinet regulations |
| Rīkojums | Order | Cabinet/ministerial orders |
| Instrukcija | Instruction | Cabinet instructions |
| ST nolēmums | CC Decision | Constitutional Court decisions |

### Issuing Bodies

- Saeima (Parliament)
- Ministru kabinets (Cabinet of Ministers)
- Valsts prezidents (President)
- Latvijas Banka (Bank of Latvia)
- FKTK (Financial and Capital Market Commission)
- Municipal governments

## Technical Details

### Discovery Methods

1. **Sitemap:** `https://likumi.lv/sitemap.xml` (~49K document URLs)
2. **RSS Feeds:** Multiple category-specific feeds for updates

### Document Access

Individual documents accessible at:
```
https://likumi.lv/doc.php?id={document_id}
```

### RSS Feeds

| Feed | URL | Content |
|------|-----|---------|
| All acts | `/rss/visi_ta.xml` | All legal acts |
| Laws | `/rss/likumi.xml` | Parliamentary laws |
| Regulations | `/rss/mk_not.xml` | Cabinet regulations |
| Orders | `/rss/mk_rik.xml` | Cabinet orders |
| CC Decisions | `/rss/st_nolemumi.xml` | Constitutional Court |

## Usage

```bash
# Test connectivity
python bootstrap.py test

# Fetch sample data (12 records)
python bootstrap.py bootstrap --sample

# Full bootstrap (all documents)
python bootstrap.py bootstrap

# Incremental update (from RSS)
python bootstrap.py update
```

## Data Format

Each normalized record contains:

```json
{
  "_id": "366190",
  "_source": "LV/LegislativeDatabase",
  "_type": "legislation",
  "title": "Grozījumi likumā \"Par aviāciju\"",
  "text": "Full text of the law...",
  "date": "2026-02-04",
  "url": "https://likumi.lv/doc.php?id=366190",
  "document_type": "Likums",
  "issuer": "Saeima",
  "doc_number": "...",
  "adopted_date": "2026-02-04",
  "effective_date": "2026-02-10",
  "language": "lv"
}
```

## Rate Limiting

- 1 request per 2 seconds (0.5 req/sec)
- Burst: 3 requests

## Notes

- Full text is extracted from HTML content (P tags)
- Consolidated versions available for all legislation
- Cross-references to EU regulations via EUR-Lex links
- Dates converted from DD.MM.YYYY to ISO 8601 format
