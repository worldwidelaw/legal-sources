# CN/MOJ-AdminRegs — China Ministry of Justice Administrative Regulations Database

**Source:** [国家行政法规库](http://xzfg.moj.gov.cn/)
**Type:** Legislation
**Records:** ~611 currently effective State Council administrative regulations
**Language:** Chinese

## Description

The National Administrative Regulations Database (国家行政法规库) is maintained by the
Ministry of Justice of the People's Republic of China. It contains the authoritative
collection of State Council administrative regulations with full text, revision history,
and metadata.

This source is distinct from CN/StateCouncil (which fetches from flk.npc.gov.cn). The
MOJ database provides versioned historical texts and tracks revisions across multiple
amendments.

## Data Access

- **List endpoint:** `https://xzfg.moj.gov.cn/SearchTitleFront?SiteID=122&PageIndex={page}`
- **Detail endpoint:** `https://xzfg.moj.gov.cn/front/law/detail?LawID={id}`
- **Download:** `https://xzfg.moj.gov.cn/law/download?LawID={id}&type=pdf`
- No authentication required

## License

[PRC Copyright Law Art. 5](http://www.npc.gov.cn/npc/c2/c30834/202011/t20201111_306832.html) — Laws, regulations, resolutions, decisions, orders, and other legislative, administrative, and judicial documents are not subject to copyright protection under Chinese law. Government open data, commercial use permitted.
