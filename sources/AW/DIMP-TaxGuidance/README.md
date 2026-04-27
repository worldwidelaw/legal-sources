# AW/DIMP-TaxGuidance

**Aruba Departamento di Impuesto Tax Guidance**

## Source

- URL: https://www.impuesto.aw
- Type: doctrine
- Auth: none (open data)
- Languages: Dutch, Papiamento

## Content

Tax guidance from Aruba's tax authority covering:
- Winstbelasting (profit tax, 25%)
- BBO/BAVP/BAZV (turnover tax and social levies)
- Inkomstenbelasting (income tax)
- Dividendbelasting (dividend tax)
- Toeristenheffing (tourist levy)
- Grondbelasting (land tax)
- Fiscal unity rules
- International tax matters
- Free zone incentives

## Method

WordPress REST API is blocked (403). Uses:
1. Sitemap.xml parsing (625 URLs)
2. HTML scraping of guidance pages
3. PDF extraction from CDN (cuatro.sim-cdn.nl/impuesto/uploads/)

## Usage

```bash
python bootstrap.py test               # Connectivity check
python bootstrap.py bootstrap --sample # Fetch ~15 sample records
python bootstrap.py bootstrap          # Full pull (~400+ pages + PDFs)
python bootstrap.py update             # Recent news items
```

## License

[Open Government Data](https://www.impuesto.aw) — official tax guidance published by the Departamento di Impuesto of Aruba.
