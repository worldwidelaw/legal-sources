# TH/Krisdika — Office of the Council of State Thai Laws

## Source
- **Dataset**: [pythainlp/thailaw](https://huggingface.co/datasets/pythainlp/thailaw) on HuggingFace
- **Coverage**: 42,755 Thai laws (Acts, Royal Decrees, Ministerial Regulations, ordinances)
- **Origin**: Office of the Council of State (Krisdika / สำนักงานคณะกรรมการกฤษฎีกา)
- **License**: CC0 1.0 (public domain)

## Data Access
HuggingFace datasets-server REST API (Parquet-backed, paginated):
```
GET https://datasets-server.huggingface.co/rows?dataset=pythainlp/thailaw&config=default&split=train&offset=0&length=100
```

Fields: `sysid` (numeric ID), `title`, `txt` (full text)

## Usage
```bash
python bootstrap.py test              # Test connectivity
python bootstrap.py bootstrap --sample # Fetch 15 sample records
python bootstrap.py bootstrap          # Full bootstrap (42,755 records)
```

## License

[CC0 1.0 (Public Domain)](https://creativecommons.org/publicdomain/zero/1.0/) — HuggingFace dataset. Underlying legislation from the Office of the Council of State of Thailand (Krisdika).
