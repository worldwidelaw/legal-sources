# CN/NPC-LocalLaws — China Local Laws and Regulations (地方性法规)

**Source:** [https://flk.npc.gov.cn/](https://flk.npc.gov.cn/)
**Data types:** legislation

## Access

The search API (`/law-search/search/list`) lists ~27,000 local and provincial
regulations newest-first; `/law-search/download/pc` returns a signed URL to the
document file.

Its `format=docx` parameter is decorative — the endpoint always serves whichever
file the publisher stored, and roughly a fifth of the corpus (concentrated in
filings before ~2023) is Word 97-2003 binary rather than DOCX. `bootstrap.py`
dispatches on the file's magic bytes: DOCX via stdlib `zipfile`, legacy `.doc`
via `common.doc_extract` (needs `olefile`), and anything else is logged and
skipped. The OSS bucket rejects unsigned requests, so the `.ofd` companion file
listed in the details response is not reachable as a fallback.

Full runs stream to `data/records.jsonl` and record every processed ID in
`data/seen_ids.txt`, so a run cut off by the wall clock resumes instead of
re-crawling from page 1.

## License

PRC Government open data — Chinese government information is publicly available under the [Regulations on Open Government Information](http://www.gov.cn/zhengce/content/2019-04/15/content_5382991.htm) (State Council Order No. 711). Free to access and use with attribution.
