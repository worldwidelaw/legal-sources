# Montenegro Constitutional Court (Ustavni sud Crne Gore)

Case law from the Constitutional Court of Montenegro.

## Source Information

- **Website**: http://www.ustavnisud.me
- **Data Type**: Case law (constitutional complaints, constitutional reviews)
- **Coverage**: 1964 to present (18,000+ decisions)
- **Language**: Serbian (sr)
- **License**: Public

## Data Access

The source uses a DataTables server-side API at `/ustavnisud/upit.php`:

- **Method**: POST
- **Format**: JSON
- **Pagination**: Standard DataTables format (start, length, draw)
- **Full text**: Available in `sadrzaj_fajlova` field

## Fields

| Field | Description |
|-------|-------------|
| iddok | Internal document ID |
| djelovodni_broj | Case number (e.g., U-III br.383/25) |
| datum | Decision date |
| vrsta_dokumenta | Document/case type |
| sadrzaj_fajlova | Full text of decision |
| osporeni_akt | Challenged act being reviewed |
| kljucne_rijeci_tagovi | Keywords/tags |
| clan_ustava_cg_atr19 | Constitutional articles cited |
| clan_konvencije_atr20 | ECHR Convention articles cited |
| komitent | Applicant information |
| datum_sjednice | Session date |

## Case Types

| Code | Description |
|------|-------------|
| U-I | Constitutional review of laws |
| U-II | Constitutional review of regulations |
| U-III | Constitutional complaints |
| U-IV | Competence disputes |
| U-V | Conflicts of jurisdiction |
| U-VI | Electoral disputes |
| U-VII | Prohibition of political parties |
| U-VIII | Other procedures |

## Usage

```bash
# Fetch sample data (15 records)
python3 bootstrap.py bootstrap --sample

# Fetch all records (stream to stdout as JSONL)
python3 bootstrap.py bootstrap

# Fetch updates since a specific date
python3 bootstrap.py updates --since 2024-01-01
```

## License

Open government data — publicly accessible court decisions.

## Notes

- Full text may have encoding issues (missing diacritics for č, ž, š characters)
- Average text length is ~45,000 characters per decision
- API returns up to 18,000+ records total
- Rate limiting: 1.5 second delay between requests
