# PL/SAOS - System Analizy Orzeczeń Sądowych

Aggregated Polish court judgments from SAOS — all court types including Supreme Court,
common courts (appeal, regional, district), Supreme Administrative Court,
Constitutional Tribunal, and National Appeal Chamber.

## Data Source

- **API**: https://www.saos.org.pl/api
- **Documentation**: https://www.saos.org.pl/help/index.php/dokumentacja-api
- **Web Interface**: https://www.saos.org.pl/search
- **Coverage**: 500,000+ judgments with full text across all Polish courts

## API Endpoints Used

| Endpoint | Purpose |
|----------|---------|
| `/api/dump/judgments` | Paginated dump of all judgments with full text |
| `/api/dump/judgments?sinceModificationDate=...` | Incremental updates |

## Court Types

| Code | Court |
|------|-------|
| `COMMON` | Common courts (sądy powszechne) — appeal, regional, district |
| `SUPREME` | Supreme Court (Sąd Najwyższy) |
| `ADMINISTRATIVE` | Supreme Administrative Court (Naczelny Sąd Administracyjny) |
| `CONSTITUTIONAL_TRIBUNAL` | Constitutional Tribunal (Trybunał Konstytucyjny) |
| `NATIONAL_APPEAL_CHAMBER` | National Appeal Chamber (Krajowa Izba Odwoławcza) |

## Usage

```bash
python bootstrap.py test-api               # Quick API connectivity test
python bootstrap.py bootstrap --sample     # Fetch sample records (12+)
python bootstrap.py bootstrap              # Full bootstrap (500K+ records)
python bootstrap.py update                 # Incremental update
```

## License

[Public Domain](https://dane.gov.pl) — Polish court decisions are public domain. SAOS is operated by ICM (University of Warsaw) in partnership with the Ministry of Justice.
