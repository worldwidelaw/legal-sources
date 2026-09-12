# NZ/HPDT — Health Practitioners Disciplinary Tribunal Decisions

Decisions of the New Zealand Health Practitioners Disciplinary Tribunal on
disciplinary charges laid against health practitioners under Part 4 of the
Health Practitioners Competence Assurance Act 2003. The Tribunal covers all
regulated professions — medical practitioners, nurses, midwives, pharmacists,
dentists and oral health, physiotherapists, psychologists, podiatrists,
chiropractors, medical laboratory scientists, occupational therapists and more.

- **Coverage:** 2004/2005 to present, ~1,500 decisions
- **Type:** `case_law`
- **Language:** English
- **Auth:** none

## Access strategy

There is no API or bulk download. `/Search-Decisions` is a DNN/XModPro grid:

1. Read the file-year filter's own option values (`2025` … `2004/2005`) from the
   search form rather than generating a year range — the oldest option is the
   combined `2004/2005`, and a hardcoded range would silently drop next year's
   decisions once the Tribunal adds the option.
2. Query one file-year at a time via the URL (`?...&YEAR=2012&...`). The whole
   corpus is reachable in one grid, but that is a ~75-page **postback** chain
   where a single dropped `__VIEWSTATE` costs the rest of the crawl; per-year
   queries are addressable by URL and run 1–5 pages each.
3. Page within a year by POSTing `__EVENTTARGET=...lnkNext` with the viewstate
   from the previous response. Rows are deduplicated on file number, since the
   year filter matches the file number rather than the decision date (a `25`
   file can be decided in 2026).
4. Each row links `/Charge-Details?file={ref}`, which names the decision PDFs in
   `.DecF1`..`.DecF6` anchors whose `href` is **empty in the HTML** — jQuery
   fills it in as `https://www.hpdt.org.nz/portals/0/{filename}`. That path is
   reconstructed here rather than executing the page's JavaScript.
5. Decision PDFs are born-digital back to 2005; penalty and appeal decisions on
   the same file are concatenated into one record.

Sample decisions run 7K–119K characters of full text.

Discovery fails loud: a year whose first page yields no rows, or 8 consecutive
failures paging within a year, aborts the run rather than reporting a truncated
corpus.

## Usage

```bash
python bootstrap.py test-api             # connectivity + parse check
python bootstrap.py bootstrap --sample   # 15 samples, one per file-year
python bootstrap.py bootstrap-fast       # full corpus (fleet entry point)
```

## Notes

- File numbers carry stray spaces in the grid (`Nur 22/565P`); `_id` normalises
  them (`hpdt-nur22-565p`).
- Decisions are subject to statutory name-suppression orders. Suppressed
  practitioners appear as "Dr Y" / "Ms C"; the published PDFs are already
  redacted by the Tribunal.

## robots.txt

⚠️ Worth an admin decision. hpdt.org.nz serves the **stock DNN robots.txt**,
which disallows the framework directories. Its `User-agent: *` group disallows
`/Portals/`; only the `Googlebot` group additionally disallows the lowercase
`/portals/` — which is the path the site's own JavaScript uses to link decision
PDFs. Robots.txt paths are case-sensitive, so as written the decision PDFs are
not disallowed for this crawler, and the separate lowercase line in the
Googlebot group suggests the operator knew the two differ. The index and detail
pages (`/Search-Decisions`, `/Charge-Details`) are not disallowed at all.

The crawler runs at 1 req/sec. The `Crawl-delay: 5` directives apply to the
`msnbot`, `Slurp` and `Googlebot` groups, not to `*`.

If the Tribunal intends `/Portals/` to cover its asset store case-insensitively,
this source should be re-marked `blocked` with reason `robots_txt_disallow`.

## License

> ⚠️ **Commercial use restricted.** The Tribunal publishes no re-use licence.

[HPDT terms of use](https://www.hpdt.org.nz/Terms) — the site carries a bare
"© Copyright, New Zealand Health Practitioners Disciplinary Tribunal" notice
and a stock DNN terms template reserving all rights. New Zealand's NZGOAL
framework would default Crown entity material to CC BY, but HPDT does not say
so, so commercial use is flagged as restricted pending written confirmation
from the Tribunal.
