# AL/SupremeCourt - Albanian Supreme Court (Gjykata e Lartë)

Court decisions from the Supreme Court of Albania.

## Data Source

**Website:** https://www.gjykataelarte.gov.al

The Supreme Court uses a Gatsby static site with a Strapi backend. Documents are stored on AWS S3.

## Data Access

Two main sources of decisions:

### 1. Archive 1999-2019
- **Endpoint:** `/page-data/sq/vendimet-e-gjykates/vendimet-1999-2019/page-data.json`
- **Format:** Monthly bundle documents (.doc) containing multiple decisions
- **Organization:** By year and month, plus United Colleges (Kolegjet e Bashkuara) compilations
- **Coverage:** ~234 unique documents spanning 20 years

### 2. Bulletins 2020+
- **Endpoint:** `/page-data/sq/lajme/buletini/page-data.json`
- **Format:** Individual decision files (.doc) embedded in periodic and thematic bulletins
- **Coverage:** Recent decisions organized by legal topic

## Court Structure

- **Kolegji Civil** - Civil College
- **Kolegji Penal** - Penal College
- **Kolegji Administrativ** - Administrative College
- **Kolegjet e Bashkuara** - United Colleges

## Usage

```bash
# Sample bootstrap (15 records)
python3 bootstrap.py bootstrap --sample

# Full bootstrap
python3 bootstrap.py bootstrap

# Fetch recent updates
python3 bootstrap.py fetch_updates --since 2024-01-01
```

## Technical Notes

- Text extraction uses `textutil` (macOS native) for .doc files
- Documents are downloaded from `gjykata-media.s3.eu-central-1.amazonaws.com`
- Bundle documents are split by the `REPUBLIKA E SHQIPERISE / GJYKATA E LARTE` header pattern
- Rate limiting: 1 request per second

## Sample Data

15 sample records with:
- Average text length: ~4,500 characters
- Full decision text including legal reasoning
- Metadata: decision number, date, college, parties

## License

Open government data. No explicit license specified.
