# DE/BremenCaseLaw - Bremen State Court Decisions

Court decisions from four Bremen courts, fetched via their SixCMS-based decision overview pages.

## Courts Covered

| Code | Court | Decisions |
|------|-------|-----------|
| OLG | Hanseatisches Oberlandesgericht Bremen | ~596 |
| OVG | Oberverwaltungsgericht Bremen | ~682 |
| VG | Verwaltungsgericht Bremen | ~696 |
| LArbG | Landesarbeitsgericht Bremen | ~59 |

## Data Access

Each court publishes decisions on their own website. The scraper:
1. Paginates through overview pages (`?skip=X&max=100`)
2. Extracts metadata (date, case number, norms, legal area, decision type)
3. Downloads PDFs from `/sixcms/media.php/13/` endpoints
4. Extracts full text via pdfplumber

## Usage

```bash
# Sample data
python3 bootstrap.py bootstrap --sample

# Full bootstrap
python3 bootstrap.py bootstrap

# Check status
python3 bootstrap.py status
```

## License

Public domain under German law — [§ 5 UrhG](https://www.gesetze-im-internet.de/urhg/__5.html) (official works / amtliche Werke).
