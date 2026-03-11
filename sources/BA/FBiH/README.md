# BA/FBiH - Federation of BiH Legislation

Federation of Bosnia and Herzegovina (Federacija Bosne i Hercegovine) federal laws
from the Official Gazette "Službene novine FBiH".

## Source

- **Website**: https://fbihvlada.gov.ba/bs/zakoni
- **Type**: HTML scraping (government website)
- **Authentication**: None required
- **License**: Public government data

## Coverage

- **Time Range**: 2019 - present
- **Document Types**: Federal laws (zakoni)
- **Languages**: Bosnian, Croatian, Serbian (bs, hr, sr)
- **Estimated Volume**: ~30-50 laws per year

## Data Access

The FBiH Government website provides chronological registers of laws by year.
Each law has a dedicated page with full text in HTML format.

The official gazette portal (sluzbenilist.ba) requires a paid subscription
for FBiH content, so we use the free government website instead.

## Usage

```bash
# List laws for a specific year
python bootstrap.py list --year 2025

# Fetch sample documents
python bootstrap.py bootstrap --sample --count 12
```

## Related Sources

- **BA/SluzbenGlasnik**: State-level legislation (BiH)
- **BA/Brcko**: Brčko District legislation
- **BA/RS**: Republika Srpska legislation (to be added)

## Notes

- FBiH is one of the two entities of Bosnia and Herzegovina
- Has its own Parliament with two chambers
- 10 cantons have own cantonal legislation (not covered here)
- Entity courts and Constitutional Court decisions need separate sources
