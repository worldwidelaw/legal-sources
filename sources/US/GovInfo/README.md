# US/GovInfo - GovInfo Federal Legislation

Data source for US federal legislation via the GovInfo Bulk Data Repository.

## Collections

- **BILLS**: Congressional Bills (113th Congress onward)
- **CFR**: Code of Federal Regulations (1996 to present)
- **PLAW**: Public and Private Laws
- **FR**: Federal Register

## Data Access

This source uses the **bulk data repository** which requires **no API key**.

- Bulk Data URL: `https://www.govinfo.gov/bulkdata`
- Documentation: https://github.com/usgpo/bulk-data
- Format: XML files with full text

## Usage

```bash
# Fetch sample records (11+ total from multiple collections)
python bootstrap.py bootstrap --sample

# Validate samples
python bootstrap.py validate
```

## Data Coverage

- Bills: Congressional bills from 113th Congress (2013) to present
- CFR: All 50 titles of federal regulations from 1996
- Public Laws: Enacted legislation by Congress

## Full Text

Documents are fetched as XML from the bulk data repository.
Full text is extracted and cleaned from XML structure.
No API key required - all data is freely accessible.

## Rate Limits

Be respectful with request frequency. The script uses 0.3s delay between requests.

## License

[Public domain](https://www.law.cornell.edu/uscode/text/17/105) — US government works under 17 U.S.C. § 105.
