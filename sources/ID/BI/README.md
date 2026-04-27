# ID/BI — Bank Indonesia Regulations

Bank Indonesia (BI) regulations covering monetary policy, banking regulation,
payment systems, and financial market regulation. Published at
[JDIH Bank Indonesia](https://jdih.bi.go.id/Web/DaftarPeraturan).

## Document Types

| Code | Name | Description |
|------|------|-------------|
| PBI | Peraturan Bank Indonesia | Primary regulations issued by BI |
| PADG | Peraturan Anggota Dewan Gubernur | Board of Governors implementation rules |
| SE | Surat Edaran | Circular letters |
| UU | Undang-Undang | Laws related to BI |

## Data Access

- **API**: REST JSON at `jdih.bi.go.id/api/WebJDIH/`
- **Listing**: All regulation IDs from `/Web/DaftarPeraturan`
- **Detail**: `/api/WebJDIH/GetDataWebPeraturan?PeraturanID={id}`
- **PDF**: `/api/WebJDIH/DownloadFilePeraturan/{id}`
- **Full text**: Extracted from PDFs via pdfminer
- **Language**: Indonesian (Bahasa Indonesia)
- **Coverage**: ~1000+ regulations, 1992 to present

## License

[Indonesia Public Domain (Government Works)](https://www.bi.go.id/id/tentang-bi/profil/Pages/Visi-Misi-dan-Nilai-Strategis.aspx) — Indonesian government regulations are public domain under Indonesian copyright law (UU Hak Cipta No. 28/2014, Article 43).
