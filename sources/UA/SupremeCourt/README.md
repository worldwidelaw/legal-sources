# UA/SupremeCourt - Supreme Court of Ukraine

Data source for the Supreme Court of Ukraine (Верховний Суд) and all cassation-level courts.

## Data Source

Uses bulk CSV downloads from [data.gov.ua](https://data.gov.ua) - the official Ukrainian Open Data Portal.

- **Dataset**: Unified State Register of Court Decisions (Єдиний державний реєстр судових рішень)
- **2026 Dataset**: https://data.gov.ua/dataset/16ab7f06-7414-405f-8354-0a492475272d
- **Full Text**: RTF files from http://od.reyestr.court.gov.ua
- **License**: Creative Commons Attribution 4.0 International

## Coverage

Cassation courts (instance_code = 1):

| Court Code | Name (Ukrainian) | Name (English) |
|------------|------------------|----------------|
| 9901 | Верховний Суд | Supreme Court |
| 9951 | Велика Палата Верховного Суду | Grand Chamber of SC |
| 9911 | Касаційний господарський суд ВС | Commercial Cassation Court |
| 9921 | Касаційний адміністративний суд ВС | Administrative Cassation Court |
| 9931 | Касаційний цивільний суд ВС | Civil Cassation Court |
| 9941 | Касаційний кримінальний суд ВС | Criminal Cassation Court |
| 5001 | Вищий господарський суд України | High Commercial Court |
| 9991 | Вищий адміністративний суд України | High Administrative Court |
| 9992 | Вищий спеціалізований суд | High Specialized Court |
| 9999 | Верховний Суд України | Supreme Court of Ukraine (old) |

## Data Fields

| Field | Description |
|-------|-------------|
| `_id` | Unique identifier (UA-SC-{doc_id}) |
| `title` | Auto-generated title from judgment type, case number, date |
| `text` | **Full text** of the court decision (extracted from RTF) |
| `date` | Adjudication date |
| `url` | Link to public registry page |
| `court` | Court name |
| `case_number` | Case reference number |
| `judgment_type` | Type of decision (Verdict, Resolution, etc.) |
| `jurisdiction` | Area of law (civil, criminal, commercial, administrative) |
| `judge` | Judge name(s) |

## Usage

```bash
# Test API connectivity
python bootstrap.py test-api

# Fetch sample records (15 documents)
python bootstrap.py bootstrap --sample

# Full bootstrap (all Supreme Court decisions for current year)
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — free reuse with attribution.

## Rate Limits

- No documented rate limits
- Conservative default: 2 requests/second
- RTF downloads: 0.5 second delay between requests
