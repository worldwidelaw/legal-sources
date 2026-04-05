# BD/SupremeCourt-Judgments — Bangladesh Supreme Court Judgments

**Source**: [Supreme Court of Bangladesh](https://www.supremecourt.gov.bd)
**Type**: Case law
**Coverage**: Appellate Division (~354 judgments) + High Court Division (~8,700+ judgments)
**Language**: English (primary), Bengali
**Auth**: None (public court records)

## Data Access

Judgments are listed on paginated HTML pages at:
- Appellate Division: `?page=judgments.php&menu=00&div_id=1&start=0`
- High Court Division: `?page=judgments.php&menu=00&div_id=2&start=0`

Each judgment is available as a PDF at `/resources/documents/{filename}.pdf`.
Full text is extracted from PDFs using PyMuPDF.

## Usage

```bash
python bootstrap.py bootstrap --sample   # Fetch sample records
python bootstrap.py bootstrap            # Full fetch
python bootstrap.py update               # Recent updates only
```
