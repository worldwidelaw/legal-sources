# ID/BI — Bank Indonesia Regulations

Bank Indonesia (BI) regulations covering monetary policy, macroprudential policy,
banking regulation, payment systems and rupiah currency management. Published in the
official [Bank Indonesia "Peraturan" section](https://www.bi.go.id/id/publikasi/peraturan).

## Document Types

| Code | Name | Description |
|------|------|-------------|
| PBI | Peraturan Bank Indonesia | Primary regulations issued by BI |
| PADG | Peraturan Anggota Dewan Gubernur | Board of Governors implementation rules |
| SE | Surat Edaran | Circular letters |

## Data Access

There is no public API. The listing is an ASP.NET WebForms page (SharePoint webpart):

- **Listing**: `/id/publikasi/peraturan/default.aspx` — filter to one calendar year via
  `TextBoxDateStart` / `TextBoxDateEnd` (DD/MM/YYYY) + `ButtonFilter`, then walk the
  `DataPagerPeraturan` "Next" image button. Every postback needs that page's own
  `__VIEWSTATE`, so hidden fields are re-read on each hop.
- **Detail**: `/id/publikasi/peraturan/Pages/{slug}.aspx`
- **PDF**: linked from the detail page under `/id/publikasi/peraturan/Documents/`.
  Records before ~2013 carry a GUID filename prefix; `FAQ_` and `Ringkasan_`
  attachments are companions and are excluded.
- **Full text**: extracted from born-digital PDFs via `common.pdf_extract`
- **Language**: Indonesian (Bahasa Indonesia)
- **Coverage**: ~65 regulations/year, 2006 to present (~1,300 documents)

### Not jdih.bi.go.id

The JDIH BI host (`jdih.bi.go.id`, including its `/api/WebJDIH/` endpoints) sits behind
an F5/BIG-IP WAF that answers **every** request from a non-Indonesian address with
HTTP 200 and a "The requested URL was rejected" interstitial. Because the status code is
200, the previous scraper parsed zero regulation IDs and reported a successful but empty
crawl ([issue #1470](https://github.com/ZachLaik/LegalDataHunter/issues/1470)). Re-verified
2026-08-21 from two independent vantages, with and without a browser User-Agent, against
`/`, `/Web/DaftarPeraturan` and the JSON API — all rejected. `www.bi.go.id` publishes the
same regulations and is reachable, so the scraper reads from there.

Discovery now fails loud: a WAF interstitial, a missing pager webpart, or a zero-row
result raises instead of silently yielding an empty corpus.

## License

[Indonesia Public Domain (Government Works)](https://www.bi.go.id/id/tentang-bi/profil/Pages/Visi-Misi-dan-Nilai-Strategis.aspx) — Indonesian government regulations are public domain under Indonesian copyright law (UU Hak Cipta No. 28/2014, Article 43).
