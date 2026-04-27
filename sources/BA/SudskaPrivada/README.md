# BA/SudskaPrivada - BiH Judicial Practice Portal

Fetches case law from the Bosnia & Herzegovina Judicial Practice Portal (Sudska praksa).

## Data Source
- **URL:** https://sudskapraksa.pravosudje.ba
- **Type:** REST API + PDF download
- **Coverage:** ~15,000 decisions from 4 highest courts
- **Courts:** Court of BiH, Supreme Court RS, Supreme Court FBiH, Appeals Court Brčko
- **Language:** Bosnian/Croatian/Serbian
- **Auth:** None required

## License

Open government data — publicly accessible court decisions.

## Usage
```bash
python bootstrap.py bootstrap --sample        # Fetch 12 sample records
python bootstrap.py bootstrap                  # Full bootstrap
python bootstrap.py update                     # Fetch recent updates
```
