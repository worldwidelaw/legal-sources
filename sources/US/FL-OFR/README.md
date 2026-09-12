# US/FL-OFR — Florida Office of Financial Regulation, Final Orders

Final orders of the **Florida Office of Financial Regulation (OFR)**, the state agency
that licenses, examines and disciplines banks, credit unions, trust companies,
money-services businesses, consumer finance companies, mortgage lenders and securities
dealers and associated persons in Florida.

Orders are published through the **DOAH Florida Administrative Index Online (FLAIO)**
at `doah.state.fl.us`, which indexes each agency's final orders by year.

## Data

- **Type:** `case_law` (agency adjudication / final orders)
- **Coverage:** OFR final orders indexed by FLAIO
- **Full text:** yes — extracted from the born-digital order PDFs
- **Sample:** 12 records, 8,192–78,178 chars (avg ~19.7K), all ISO-dated

## Access

Plain HTTPS. The FLAIO index pages list one row per order with a link to the order PDF
under `https://www.doah.state.fl.us/FLAID/OFR/{YYYY}/...pdf`. No login, no API key.

```bash
python bootstrap.py test               # Print discovered order entries
python bootstrap.py bootstrap --sample # Save 12 sample records
python bootstrap.py bootstrap          # Full corpus -> data/records.jsonl
```

## License

[Public Domain — US state government work](https://www.law.cornell.edu/uscode/text/17/105) — orders of the Florida Office of Financial Regulation are official works of Florida state government and are not subject to copyright under the government-edicts doctrine. Commercial use permitted; no attribution required.
