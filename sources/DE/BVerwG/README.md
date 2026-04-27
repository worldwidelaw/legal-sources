# DE/BVerwG — German Federal Administrative Court

## Source Information

- **Court**: Bundesverwaltungsgericht (BVerwG)
- **Type**: Case Law
- **Language**: German
- **License**: Public Domain ([§ 5 UrhG](https://www.gesetze-im-internet.de/urhg/__5.html))
- **Estimated Records**: ~10,000+

## Data Access

Official open data from [rechtsprechung-im-internet.de](https://www.rechtsprechung-im-internet.de), the same platform used for BGH, BVerfG, and BSG.

### Endpoints

1. **Table of Contents (TOC)**: `https://www.rechtsprechung-im-internet.de/rii-toc.xml`
   - Contains metadata for all decisions across all federal courts
   - Filter by `<gericht>` starting with "BVerwG"

2. **Decision ZIP files**: `http://www.rechtsprechung-im-internet.de/jportal/docs/bsjrs/jb-{DOC_ID}.zip`
   - Contains structured XML with full decision text

3. **RSS Feed**: `https://www.rechtsprechung-im-internet.de/jportal/docs/feed/bsjrs-bverwg.xml`
   - Recent decisions only

## Schema

| Field | Description |
|-------|-------------|
| `_id` | Document number (doknr) |
| `ecli` | European Case Law Identifier |
| `aktenzeichen` | Case reference number |
| `date` | Decision date (ISO 8601) |
| `court` | Court name (BVerwG) |
| `chamber` | Senate/chamber (e.g., "8. Senat") |
| `decision_type` | Type (Urteil, Beschluss, etc.) |
| `text` | Full decision text |
| `headnote` | Leitsatz (summary) |
| `tenor` | Operative part |
| `norms` | Referenced legal norms |

## Usage

```bash
# Test (fetch 3 decisions)
python3 bootstrap.py

# Bootstrap sample (100 decisions)
python3 bootstrap.py bootstrap --sample
```

## License

Public domain under German law — [§ 5 UrhG](https://www.gesetze-im-internet.de/urhg/__5.html) (official works / amtliche Werke).
