# CN/PBOC — People's Bank of China Regulations

Central bank regulations, rules, and normative documents from the
People's Bank of China (中国人民银行).

## Sections

| Section | Type | ~Count |
|---------|------|--------|
| National Laws (国家法律) | legislation | 25 |
| State Council Regulations (国务院条例) | legislation | 8 |
| PBC Rules/Orders (人民银行令) | legislation | 113 |
| Announcements/Normative Docs (公告) | doctrine | 424 |

**Total: ~570 documents**

## Usage

```bash
# Test connectivity
python bootstrap.py test-api

# Fetch sample (4 per section = 16 records)
python bootstrap.py bootstrap --sample

# Fetch all documents
python bootstrap.py bootstrap --full

# Fetch updates since a date
python bootstrap.py updates --since 2025-01-01
```

## Data Access

HTML scraping of pbc.gov.cn. Full text embedded in detail pages.
No API key required. Chinese language (Simplified).

## License

[Open Government Data](http://www.pbc.gov.cn) — official regulations and normative documents published by the People's Bank of China.
