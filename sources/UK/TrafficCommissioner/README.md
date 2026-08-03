# Traffic Commissioners for Great Britain — Regulatory Decisions

Regulatory decisions of the **Traffic Commissioners for Great Britain**,
published on GOV.UK. The Traffic Commissioners are the independent regulators
of the heavy goods vehicle (HGV) and public service vehicle (PSV / bus & coach)
industries and of local bus service registration in England, Scotland and Wales.

## Coverage

- **~185 decisions** (`traffic_commissioner_regulatory_decision` format on GOV.UK)
- Outcomes include: operator licence revocation, curtailment, suspension,
  formal warnings, loss of repute, transport-manager and licence-holder
  disqualification, and conditions imposed on licences.
- `_type`: `case_law` (quasi-judicial regulatory adjudications)

## Data access

Uses the GOV.UK finder + Content Store pattern (shared with `UK/ET`, `UK/RPT`,
`UK/FTT-Tax`), but with one difference: the full decision text for this format
lives in the Content Store **`details.body`** field (HTML), because
`hidden_indexable_content` is empty for traffic commissioner decisions.

1. **Discovery** — `GET /api/search.json?filter_format=traffic_commissioner_regulatory_decision`
2. **Full text** — `GET /api/content/{link}` → `details.body`, stripped to clean
   plain text. Structured metadata (`case_type`, `decision_subject`,
   `outcome_type`, `regions`, `first_published_at`) comes from
   `details.metadata`.

No authentication, no CAPTCHA, no IP gate.

## Usage

```bash
python bootstrap.py bootstrap --sample   # 12 validation samples
python bootstrap.py bootstrap            # full pull
python bootstrap.py bootstrap-fast       # alias for full pull (fleet runner)
```

## License

[Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/) — Crown copyright; attribution required, commercial use permitted.
