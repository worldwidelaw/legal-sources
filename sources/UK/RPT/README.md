# UK First-tier Tribunal (Property Chamber) — Residential Property Decisions

Decisions of the **First-tier Tribunal (Property Chamber) — Residential Property**
(England), published on GOV.UK. The Chamber is the successor to the Residential
Property Tribunal Service and the Rent Assessment Committees.

## Coverage

- **~17,200 decisions** (`residential_property_tribunal_decision` format on GOV.UK)
- Subject matter: rents (fair rent / market rent), service charges, leasehold
  enfranchisement and lease extensions, right to manage, park homes (pitch fees),
  HMO and selective licensing, rent repayment orders, banning orders, and civil
  financial penalties under the Landlord and Tenant Acts, Housing Act 2004 and
  Housing and Planning Act 2016.
- `_type`: `case_law`

## Data access

Uses the same GOV.UK finder + Content Store pattern as `UK/ET` (Employment
Tribunals) and `UK/FTT-Tax` (Tax Chamber):

1. **Discovery** — `GET /api/search.json?filter_format=residential_property_tribunal_decision`
   (paginated 500/page, ordered by `-public_timestamp`).
2. **Full text** — `GET /api/content/{link}` → `details.metadata.hidden_indexable_content`
   holds the full born-digital plain text of each decision (no OCR needed).

No authentication, no CAPTCHA, no IP gate. Reachable over plain HTTPS.

## Usage

```bash
python bootstrap.py bootstrap --sample   # 12 validation samples
python bootstrap.py bootstrap            # full pull
python bootstrap.py bootstrap-fast       # alias for full pull (fleet runner)
```

## License

[Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/) — Crown copyright; attribution required, commercial use permitted.
