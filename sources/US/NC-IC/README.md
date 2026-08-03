# US/NC-IC — North Carolina Industrial Commission Decisions

Full-text decisions of the **North Carolina Industrial Commission** — the
state tribunal that adjudicates workers'-compensation claims (and tort
claims against State agencies). Each decision is an *Opinion and Award*
resolving a specific contested case = **case_law**.

- **Publisher:** North Carolina Industrial Commission
- **Site:** https://www.ic.nc.gov/database.html
- **Backend:** OpenText™ Content Server 10 SP2 (`ic.nc.gov/livelink`)
- **Type:** `case_law`
- **Jurisdiction:** US-NC
- **Coverage:** Full Commission and Deputy Commissioner opinion-and-award
  databases (decisions since 1994). Sample decisions span 1996–2024.

## Access recipe

The Commission publishes four public, full-text-searchable Content Server
databases. Access is via a **guest account — username and password are both
the literal word `public`** (as instructed on the Commission's database page).

1. `POST func=ll.login` with `Username=public&Password=public` → `LLCookie`
   session (persisted in a cookie jar).
2. `GET` the search-prompt page
   (`func=ll&objType=258&objAction=searchprompt`) and extract the byte-exact
   `template` hidden field (an OpenText `A<…>` structured argument).
3. `POST func=NewSearch` with a **minimal** field set — sending the full form
   triggers `[Pattern of argument was not recognized.]`. A `Referer`/`Origin`
   header of `ic.nc.gov` is mandatory (Content Server otherwise rejects the
   request as "potentially unsafe"). Each result links a decision as
   `func=doc.ViewDoc&nodeid=<N>&vernum=1`.
4. Fetch the decision's **"View as Web Page"** HTML render at
   `/livelink/llview.exe/<name>.html?func=doc.View&nodeId=<N>&vernum=1` and
   strip tags → full body text.

Content Server denies the guest user folder browsing, so the corpus is
enumerated by full-text searching each workers'-comp slice for a set of
ubiquitous decision terms (injury, employee, compensation, plaintiff,
defendant, …) and de-duplicating by `nodeid`. Every decision contains several
of these terms, so the union approximates the whole corpus.

**Slice (database) IDs:** Full Commission = `148754`, Deputy Commissioner =
`268779`, N.C. Supreme Court & Court of Appeals = `149085`, Enterprise (all) =
`147904`.

## Fields

`_id`, `_source`, `_type`, `_fetched_at`, `nodeid`, `case_number`
(`I.C. No.`), `database`, `parties`, `issuer`, `title`, `text` (full body),
`url`, `date` (filing date, ISO 8601), `jurisdiction`.

## Usage

```bash
python bootstrap.py test-api            # connectivity + search test
python bootstrap.py bootstrap --sample  # ~12 samples
python bootstrap.py bootstrap           # full pull
```

## License

[Public Domain (US Government Work)](https://www.law.cornell.edu/uscode/text/17/105)
— Decisions of the North Carolina Industrial Commission are official
state-government works in the public domain under the government-edicts
doctrine. Commercial use permitted; no attribution required.
