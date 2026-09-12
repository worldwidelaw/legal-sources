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

The full crawl walks the sequential `idMat` document-ID space:

1. Read the resume point (see below) to find the first ID to scan
2. Read the newest published `idMat` from the RSS feed's latest issue, and scan
   up to it plus a margin for materials issued mid-run
3. Fetch `/showMaterialDV.jsp?idMat={id}` for each ID and extract the full text
4. Store normalized records, persisting progress as it goes

Only about a third of the ID space is live, in clusters separated by dead runs of
10–40 IDs (wider bands exist, e.g. around 231300–231500), so misses are expected
and do not mean the corpus has ended.

### Resume / checkpointing

The corpus is ~245,000 IDs, which does not fit in a single fleet slot: a run from
scratch hits the 100h wall around `idMat=200000` (issue #1433). The crawl is
therefore resumable:

- `resume_point.json` — committed to git, so a **fresh clone on a new VPS**
  continues from the last completed position instead of restarting from
  `idMat=1000`. After a fleet run, copy `last_id` from that run's progress file
  into this file so the next worker picks up where this one stopped.
- `data/fetch_all_progress.json` — live progress, written every 250 scanned IDs.
  Survives restarts on the same box (gitignored, lost on teardown).

The resume point is the last ID that actually **yielded a document**, not the last
scanned — the tail past the final document is where new materials appear, so it is
re-scanned on every run. An interrupted run may re-fetch up to ~250 IDs of already
ingested ground; the loader dedups on `doc_id`.

Force a full re-crawl from the beginning with `--restart`.

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
