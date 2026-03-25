# NA/LawsAfricaActs — Namibian Legislation (NamibLII)

Fetches Namibian legislation (Acts, Ordinances, Government Notices, Proclamations) from [NamibLII](https://namiblii.org/), a PeachJam/AfricanLII platform.

## Data

- **Type**: Legislation
- **Count**: ~418 documents
- **Coverage**: 1840–2025
- **Full text**: Yes, via `la-akoma-ntoso` HTML element (Akoma Ntoso)
- **Auth**: None required

## Usage

```bash
python bootstrap.py test               # Connectivity test
python bootstrap.py bootstrap --sample # 15 sample records
python bootstrap.py bootstrap          # All ~418 acts
```

## Notes

- 5-second crawl delay per robots.txt
- Judgments and Official Gazette excluded (disallowed by robots.txt)
