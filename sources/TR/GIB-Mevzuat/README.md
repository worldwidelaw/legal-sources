# TR/GIB-Mevzuat — Turkish Revenue Administration Legislation

Tax circulars (sirküler), communiques (tebliğ), general letters, internal circulars,
presidential decrees, council decisions, regulations, and justifications from Turkey's
Revenue Administration (Gelir İdaresi Başkanlığı / GİB).

**Note:** Tax rulings (özelge) are covered separately by `TR/GIB-Ozelgeler`.

## Data

| Type | Count | Description |
|------|-------|-------------|
| Sirküler | ~578 | Tax circulars |
| Tebliğ | ~2,478 | General communiques |
| BKK | ~588 | Council of Ministers decisions |
| İç Genelge | ~323 | Internal circulars |
| CBK | ~231 | Presidential decrees |
| Gerekçe | ~93 | Legislative justifications |
| Genel Yazılar | ~91 | General correspondence |
| Yönetmelik | ~66 | Regulations |

**Total: ~4,448 documents with full text**

## API

Uses GİB's Spring Boot REST API (reverse-engineered from the Next.js frontend):

```
POST https://gib.gov.tr/api/gibportal/mevzuat/{type}/list?page=0&size=50
Content-Type: application/json
Body: {"status":2,"deleted":false}
```

No authentication required. Returns paginated JSON with full HTML content.

## License

[Turkish Government Public Domain](https://gib.gov.tr) — Official government tax legislation and guidance, public domain under Turkish law.
