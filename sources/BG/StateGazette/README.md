# Bulgarian State Gazette (Държавен вестник)

Official journal of the Republic of Bulgaria, managed by the National Assembly.

## Source Information

- **Country**: Bulgaria (BG)
- **URL**: https://dv.parliament.bg
- **Language**: Bulgarian
- **Coverage**: 2003 onwards (full-text search)
- **Data Type**: Legislation

## What This Source Provides

The Bulgarian State Gazette publishes official legislative and administrative documents:

### Official Section
- Laws from the National Assembly
- Presidential decrees and orders
- Constitutional Court decisions
- Council of Ministers resolutions and decisions
- Ministry regulations and orders

### Unofficial Section
- Municipal orders
- Court decisions
- Public procurement notices
- Corporate establishment/closure notices

## Data Access Method

1. **RSS Feeds**:
   - Official section: `/DVWeb/rss_newspaper.jsp` (recent ~7 items)
   - Public procurement: `/DVWeb/rss_porachki.jsp`

2. **Issue Pages**:
   - Each issue has a table of contents at `/materiali.faces?idObj={issue_id}`
   - Individual documents at `/showMaterialDV.jsp?idMat={doc_id}`

3. **Search Interface**:
   - Web form at `/searchDV.faces`
   - Full-text search from 2003 onwards

## Technical Notes

- SSL certificate verification fails - must use `verify=False` in requests
- Documents are published as amendments/corrections, not full legislative texts
- Content in UTF-8 encoded Bulgarian
- HTML content available for each material
- No official API - relying on RSS + HTML scraping

## Authentication

None required - all data is publicly accessible.

## Rate Limiting

Conservative approach: 1 request/second to respect the parliamentary infrastructure.

## Bootstrap Strategy

1. Fetch RSS feed for recent official section materials
2. For each RSS item, fetch full issue contents page
3. Extract document metadata from issue table
4. Match RSS and scraped data by description
5. Store normalized records

## Update Strategy

1. Fetch RSS feed
2. Filter by publication date (since last update)
3. Fetch full details for new materials

## Sample Data

Run with `--sample` flag to fetch 10 recent documents:

```bash
python3 bootstrap.py bootstrap --sample
```

Sample data will be saved to `sample/` directory.

## License

Public domain — Bulgarian official gazette publications are not subject to copyright.
