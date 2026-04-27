# IN/CCI — Competition Commission of India Orders

Fetches antitrust and combination orders from the Competition Commission of India.

## Data Source

- **URL**: https://www.cci.gov.in/
- **Type**: case_law
- **Auth**: none (session cookie + CSRF token auto-obtained)
- **Records**: ~1,300 antitrust orders + ~1,400 combination orders
- **Coverage**: 2009-present

## Endpoints

- **Antitrust**: `POST /antitrust/orders/list` (DataTables, requires CSRF token)
- **Combination**: `GET /combination/orders-section31` (DataTables)
- **PDFs**: Direct download from `cci.gov.in/images/antitrustorder/en/...`

## Usage

```bash
python bootstrap.py test               # Connectivity test
python bootstrap.py bootstrap --sample # 15 sample records
python bootstrap.py bootstrap          # Full pull (~2,700 orders)
python bootstrap.py update --days 90   # Recent orders
```

## Full Text

Full text extracted from PDF orders using pdfplumber. PDFs contain selectable text.

## License

[Open Government Data](https://www.cci.gov.in/) — official orders published by the Competition Commission of India.
