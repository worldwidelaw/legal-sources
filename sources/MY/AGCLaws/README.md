# MY/AGCLaws — Malaysia Laws of Malaysia (AGC Official)

**Source:** [https://lom.agc.gov.my/](https://lom.agc.gov.my/)
**Data types:** legislation

## Access

Two DataTables endpoints are paged 100 rows at a time:

| Endpoint | Listing page | Records |
|----------|--------------|---------|
| `POST /json-updated-2024.php` | `/principal.php?type=updated` | ~887 consolidated acts |
| `POST /json-amendment-2024.php` | `/principal.php?type=amendment` | ~406 amendment acts |

Full text comes from the linked act PDFs under `/ilims/upload/portal/akta/...`,
preferring the English (`_BI`) edition over the Malay (`_BM`) one.

### Encrypted responses (issue #1465)

Since 2026 these endpoints no longer return plain JSON. They return

```json
{"encrypted": true, "data": "<base64>"}
```

where the payload is AES-256-GCM laid out as `IV(12) || tag(16) || ciphertext`.
The 64-hex-character key is published in the listing page source as
`SEARCH_RESPONSE_KEY` and is what the site's own `js/responseCrypto.js` uses, so
decrypting is exactly what any browser does. `bootstrap.py` reads the key from
the live page each run (picking up a rotation automatically) and falls back to
the last known constant. Decryption needs `cryptography` (or `pycryptodome`).

## License

Official Malaysian legislation published by the Attorney General's Chambers. Copyright Government of Malaysia. Free public access for informational purposes; Malaysian Copyright Act 1987 applies.
