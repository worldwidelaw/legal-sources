# TR/Mevzuat - Turkish Legislation Database

## Source Information

- **Name**: Mevzuat Bilgi Sistemi (Legislation Information System)
- **URL**: https://www.mevzuat.gov.tr
- **Operator**: T.C. Cumhurbaşkanlığı (Presidency of the Republic of Turkey)
- **Language**: Turkish
- **License**: Open Government Data

## Coverage

The database contains approximately **28,900 records** across six legislation types:

| Type | Turkish Name | Count |
|------|--------------|-------|
| 1 | Kanunlar (Laws) | ~912 |
| 2 | KHK (Decree with Force of Law) | ~107 |
| 3 | Tüzükler (Regulations) | ~8,875 |
| 4 | Yönetmelikler (Directives) | ~63 |
| 5 | Cumhurbaşkanlığı Kararnameleri (Presidential Decrees) | ~185 |
| 6 | Cumhurbaşkanı Kararları (Presidential Decisions) | ~18,760 |

## API Endpoints

### DataTable API (metadata listing)
```
POST https://www.mevzuat.gov.tr/Anasayfa/MevzuatDatatable
Content-Type: application/json

{
  "draw": 1,
  "start": 0,
  "length": 100,
  "parameters": {
    "MevzuatTur": 1,
    "MevzuatTertip": 5
  }
}
```

### Full Text Iframe
```
GET https://www.mevzuat.gov.tr/anasayfa/MevzuatFihristDetayIframe
  ?MevzuatTur={type}
  &MevzuatNo={number}
  &MevzuatTertip=5
```

## Data Schema

```json
{
  "_id": "TR-kanun-7557",
  "_source": "TR/Mevzuat",
  "_type": "legislation",
  "_fetched_at": "2026-02-19T12:00:00Z",
  "title": "SAĞLIKLA İLGİLİ BAZI KANUNLARDA DEĞİŞİKLİK YAPILMASINA DAİR KANUN",
  "text": "Full text of the legislation...",
  "date": "2025-07-24",
  "url": "https://www.mevzuat.gov.tr/mevzuat?MevzuatNo=7557&MevzuatTur=1&MevzuatTertip=5",
  "mevzuat_no": "7557",
  "mevzuat_tur": 1,
  "mevzuat_tur_name": "Kanunlar (Laws)",
  "mevzuat_tertip": "5",
  "accept_date": "2025-07-21",
  "gazette_date": "2025-07-24",
  "gazette_number": "32965"
}
```

## Usage

```bash
# Fetch sample records (15 records from different types)
python3 bootstrap.py bootstrap --sample

# Fetch all records
python3 bootstrap.py bootstrap

# Fetch with limit
python3 bootstrap.py bootstrap --limit 100

# Fetch updates since a date
python3 bootstrap.py updates --since 2025-01-01
```

## Notes

- The API requires session cookies from the main page
- Rate limit: 1 request per 1.5 seconds
- Full text is extracted from HTML iframe content
- Content is in WordSection1 div (Microsoft Word export format)

## License

[Open Government Data](https://www.mevzuat.gov.tr) — official legislation published by the Presidency of the Republic of Turkey.
