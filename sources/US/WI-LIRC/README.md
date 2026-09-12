# US/WI-LIRC — Wisconsin Labor & Industry Review Commission, Decisions

Decisions of the **Wisconsin Labor and Industry Review Commission (LIRC)**, the
independent quasi-judicial commission that reviews determinations in three areas:

- **Unemployment insurance** (appeals from ALJ decisions of the Department of Workforce Development)
- **Worker's compensation** (review of ALJ decisions on injury, disability and benefit disputes)
- **Equal rights / fair employment** (discrimination, retaliation and public-accommodation claims under the Wisconsin Fair Employment Act)

LIRC decisions are the last administrative step before judicial review in the Wisconsin
circuit courts, so they are the authoritative statement of Wisconsin administrative
labour law.

## Data

- **Type:** `case_law` (quasi-judicial administrative decisions)
- **Coverage:** decisions published at `lirc.wisconsin.gov`
- **Full text:** yes — extracted from the decision PDFs
- **Sample:** 12 records, 4,908–24,918 chars

## Access

Plain HTTPS, no login and no API key. The commission publishes decisions as PDFs under
per-programme paths (e.g. `/ucdecsns/`, `/wcdecsns/`, `/erdecsns/`).

```bash
python bootstrap.py test               # Print discovered decision entries
python bootstrap.py bootstrap --sample # Save 12 sample records
python bootstrap.py bootstrap          # Full corpus -> data/records.jsonl
```

## License

[Public Domain — US state government edict](https://www.law.cornell.edu/uscode/text/17/105) — decisions of the Wisconsin Labor and Industry Review Commission are official works of Wisconsin state government (edicts of a quasi-judicial government body) and are not subject to copyright under the government-edicts doctrine. Commercial use permitted; no attribution required.
