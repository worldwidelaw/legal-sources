# IM/FSA-Enforcement — Isle of Man Financial Services Authority Enforcement

**Source:** [https://www.iomfsa.im/enforcement/](https://www.iomfsa.im/enforcement/)
**Data types:** doctrine

## Coverage

Four collections, all server-rendered HTML on the Authority's Umbraco site
(no API, no auth):

| Collection | Where | Approx. count |
|---|---|---|
| Discretionary civil penalties | `/enforcement/enforcement-action/` table | 21 |
| Prohibited persons | `/enforcement/enforcement-action/` accordions | 9 |
| Disqualified directors | `/enforcement/disqualified-directors/` accordions | 14 |
| Public warnings (s.30 Financial Services Act 2008) | `/fsa-news/?category=Public Warning` | ~590, back to 2002 |
| Public notices (s.12 Designated Businesses (Registration & Oversight) Act 2015) | `/fsa-news/?category=Public Notice` | ~500, back to 2015 |

The two `/enforcement/` pages only list measures currently in force. The
historic corpus of statutory notices lives in the `/fsa-news/` archive, which
is paginated 10 per page via `?page=N`. **An out-of-range `page` silently
re-serves page 1 rather than an empty result**, so `_iter_news_urls` detects
wrap-around by comparing each page's leading link against page 1's.

The `Press Release` and `Sanctions Notices` news categories are deliberately
out of scope: the former is general regulator news (consultations, guidance,
conference announcements) and the latter is pointers to UK OFSI sanctions
lists. Enforcement press releases linked from the civil-penalty table are
still fetched, and de-duplicated against the archive pass.

## License

Crown Copyright, Isle of Man Government. Published on [iomfsa.im](https://www.iomfsa.im/) for public access. No formal open licence specified; standard Crown Copyright terms apply.
