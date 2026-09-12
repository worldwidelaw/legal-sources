# SI/DrzavniZbor — Slovenian National Assembly Session Transcripts

Verbatim transcripts (dobesedni zapisi) of plenary sessions of the Slovenian
National Assembly (Državni zbor) from 1992 to present.

## Data Source

- **Metadata**: Parliament open data XML files at `fotogalerija.dz-rs.si/datoteke/opendata/`
- **Full text**: Parliament website transcript pages at `dz-rs.si/wps/portal/Home/seje/evidenca`

## Coverage

- Parliamentary mandates 2–10 (1992–present)
- 2,356 verbatim transcripts (one record per sitting day)
- Both regular (redna) and extraordinary (izredna) sessions
- Full verbatim text including speaker attributions

## Usage

```bash
python bootstrap.py bootstrap --sample   # 15 sample records
python bootstrap.py bootstrap --full     # All records (~2,356) -> data/records.jsonl
```

## License

[Open Government Data](https://podatki.gov.si) — Parliament open data, freely reusable.
