# RS/ConstitutionalCourt - Serbian Constitutional Court

**Source:** Serbian Constitutional Court (Ustavni sud)
**URL:** https://ustavni.sud.rs
**Data Type:** Case Law
**Coverage:** 1998 onwards (~21,000+ decisions)

## Overview

The Serbian Constitutional Court (Ustavni sud Republike Srbije) is the court authorized to perform judicial review in Serbia. It rules on whether laws, decrees, or other acts enacted by Serbian authorities are in conformity with the Constitution.

## Data Access

This scraper uses the official judicial practice database at:
- Search: `https://ustavni.sud.rs/sudska-praksa/baza-sudske-prakse`
- Detail: `https://ustavni.sud.rs/sudska-praksa/baza-sudske-prakse/pregled-dokumenta?PredmetId={id}`

### Pagination Parameters

- `startfrom`: Offset for pagination (0-indexed)
- `limit`: Number of results per page (default: 10)
- `sortBy`: Sort order (`dateDESC`, `dateASC`, `codeDESC`, `codeASC`)
- `action`: Must be `1` to get results

## Case Types

| Code | Serbian | English |
|------|---------|---------|
| Уж | Уставне жалбе | Constitutional complaints |
| IУ | Сагласност општих аката са Уставом | Constitutionality of general acts |
| IУз | Сагласност закона са Уставом | Constitutionality of laws |
| IУм | Сагласност потврђених међународних уговора | Constitutionality of treaties |
| IУо | Сагласност подзаконских аката | Constitutionality of regulations |
| IУа | Сагласност аката органа АП | Constitutionality of AP acts |
| IУл | Сагласност аката органа ЈЛС | Constitutionality of local acts |
| IУп | Сагласност уредби и подзаконских аката | Constitutionality of decrees |
| IIУ | Оцена уставности закона пре проглашења | Pre-promulgation review |
| IIIУ | Решавање сукоба надлежности | Jurisdiction conflicts |
| IVУ | Повреда Устава од стране председника | Presidential impeachment |
| VУ | Изборни спорови | Electoral disputes |
| VIУ | Жалбе на потврђивање мандата | Mandate confirmation appeals |
| VIIУ | Забрана рада организација | Organization prohibition |
| VIIIУ | Жалбе судија и тужилаца | Judge/prosecutor appeals |
| IXУ | Жалбе на појединачне акте | Individual act appeals |
| XУ | Друге надлежности | Other competencies |

## Data Fields

### Primary Fields
- `predmet_id` - Unique numeric identifier
- `case_reference` - Case number (e.g., "Уж-11232/2017")
- `title` - Constructed title
- `text` - **Full text of the decision**
- `date` - Decision date (ISO 8601)
- `url` - Direct link to document

### Metadata Fields
- `case_type` - Type of proceeding
- `outcome` - Decision outcome
- `legal_area` - Legal area/category
- `constitutional_articles` - Referenced constitutional provisions
- `applicant` - Applicant name/initials
- `notes` - Additional remarks

## Usage

```bash
# Fetch sample data (12 records)
python bootstrap.py bootstrap --sample

# Full bootstrap (all ~21,000 decisions)
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## License

Open government data — publicly accessible court decisions.

## Notes

- Language: Serbian (Cyrillic and Latin scripts)
- Full text is available on detail pages in HTML format
- No authentication required
- Rate limited to 1 request/second
- SSL certificate has issues; verification is disabled
