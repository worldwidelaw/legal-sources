# IN/CESTAT — Customs, Excise & Service Tax Appellate Tribunal

Final orders from CESTAT, India's appellate tribunal for indirect tax matters.

## Coverage

- **Benches**: Ahmedabad, Allahabad, Bangalore, Chandigarh, Chennai, Delhi, Hyderabad, Kolkata, Mumbai
- **Case types**: Customs, Excise, Service Tax, Antidumping, Central Sales Tax
- **Period**: 2015–present (earlier records may lack digital PDFs)
- **Format**: PDF orders (digitally generated, text-extractable)

## Data Access

Uses the AJAX search API at `cestat.gov.in/ajax/order-status-web` to enumerate
orders by bench and date range, then downloads individual PDFs for full text
extraction.

```bash
python3 bootstrap.py test                # connectivity probe
python3 bootstrap.py bootstrap --sample  # 15 records into sample/
python3 bootstrap.py bootstrap           # full corpus → data/records.jsonl
python3 bootstrap.py bootstrap-fast      # alias for the full corpus
python3 bootstrap.py update --days 90
```

### Document identity

`_id` is built from CESTAT's own order identifier — the trailing number in the
`weborders` link, e.g. `/weborders/file/chandigarh/328752` → `CESTAT-chandigarh-328752`.

Do **not** key on the case number. It is shared by every order issued in the
same appeal (one number appeared on 45 separate orders) and is blank on some
listings, which collapsed 219,500 ingested rows onto 58,744 distinct ids and
left re-runs inserting duplicates rather than colliding (issue #1437). Measured
on two live windows, the order id is 1:1 with documents where the case number is
not:

| Window | Rows | Distinct order id | Distinct case no |
|---|---|---|---|
| Chandigarh, Jun 2024 | 389 | 389 | 383 |
| Delhi, Apr 2026 | 1,781 | 1,781 | 1,713 |

The case number is still emitted, as `case_number`.

### Checkpointing

A full crawl is 9 benches × ~11 years of months and runs past the fleet's
100-hour cap. Completed bench-months are recorded in
`data/cestat_checkpoint.json` and skipped with no network call on restart, so
successive slots advance monotonically instead of re-walking 2015 each time.
Delete that file to force a full re-crawl.

## License

[Government Open Data](https://cestat.gov.in/) — Indian government tribunal decisions are public records. Attribution recommended.
