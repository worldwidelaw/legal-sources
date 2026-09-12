# KR/CourtCLIS — Comprehensive Legal Information System (CLIS)

**Source:** [https://www.law.go.kr/](https://www.law.go.kr/)
**Data types:** case_law

Korean court precedents (판례) from the official law.go.kr DRF API: Supreme Court,
lower courts and specialised tribunals, ~170K decisions with full opinion text
(판시사항 / 판결요지 / 판례내용).

## Endpoints

| Purpose | Call |
|---------|------|
| Listing | `GET /DRF/lawSearch.do?OC=test&target=prec&type=XML&display=100&page=N&sort=ddes` |
| Full text | `GET /DRF/lawService.do?OC=test&target=prec&type=XML&ID={판례일련번호}` |

`sort=ddes` is 선고일자-descending: page 1 holds the newest decisions, page 400
lands in 2016, and the ~29K records carrying the `00010101` placeholder date sort
to the very end. `display` is capped at 100 server-side.

## Refresh strategy

A full crawl is ~1,705 listing pages plus one detail fetch per decision, so it
needs several fleet slots. Both paths share `data/clis_checkpoint.json`:

- **Full crawl** (`bootstrap`, `bootstrap-fast`) resumes from `done_page` and
  rewinds it to 0 on completion — parked on the last page, every later full crawl
  would start past the ceiling and report zero records, which is
  indistinguishable from a dead host.
- **Refresh** (`fetch_updates`) walks newest-first from page 1 and yields only
  IDs absent from the checkpoint, stopping after 20 pages of known IDs. `since`
  sets a depth floor (plus a 400-day publication-lag allowance) so a refresh
  after a long gap sweeps the whole gap.

The comparator is a seen ID rather than 선고일자, because law.go.kr publishes a
decision months after it was handed down and 29,180 records assert no date at
all — a date cutoff would silently drop every late-published decision (#1502,
#1496).

Records added with the placeholder date sort to the listing tail and so are out
of reach of a newest-first walk; the periodic full crawl is what picks them up.

## License

[Open government data](https://www.data.go.kr)
