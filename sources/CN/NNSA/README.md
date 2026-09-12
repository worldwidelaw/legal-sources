# CN/NNSA — National Nuclear Safety Administration of China (国家核安全局)

Full-text nuclear-safety legal corpus of China's nuclear regulator, the
National Nuclear Safety Administration (NNSA), which operates inside the
Ministry of Ecology and Environment (MEE).

- **Site:** https://nnsa.mee.gov.cn/
- **Language:** Chinese (a few guides are bilingual zh/en)
- **Auth:** none
- **Access:** static listing HTML → CMS detail pages → PDF/DOCX attachments

## Coverage

| Channel | Type | Approx. size |
|---------|------|--------------|
| 国家法律 — national laws (原子能法, 核安全法, 放射性污染防治法 …) | `legislation` | 4 |
| 行政法规 — State Council administrative regulations | `legislation` | 7 |
| 规章 — NNSA/MEE departmental rules (部令) | `legislation` | 25 |
| 规范性文件 — normative documents | `legislation` | 4 |
| 国际公约 — international nuclear conventions China is party to | `legislation` | 10 |
| 核安全导则 — nuclear-safety guides, 8 series (HAD…) | `doctrine` | 109 |
| 标准 — national nuclear/radiation standards, 7 series (GB…, HJ…) | `doctrine` | 124 |
| 核安全局文件 / 文件 — NNSA decisions | `case_law` / `doctrine` | ~3,000 |
| 核安全局文件 / 函 — NNSA official letters | `case_law` / `doctrine` | ~1,180 |
| 部文件 — MEE nuclear/radiation ministerial documents | `doctrine` | ~400 |
| 其他 / 解读 — other policy documents and official interpretations | `doctrine` | ~165 |

The 文件 and 函 channels are individual administrative decisions: licence
grants, renewals, variations and revocations for nuclear installations and
for civil nuclear-safety equipment designers, manufacturers and installers;
operator-qualification decisions; approval of refuelling programmes; and
rectification orders addressed to a named licensee. A row in those channels
is classified `case_law` when its title carries a decisional verb
(颁发/换发/延续/注销/吊销/批准/同意/准予/责令/处罚/整改/不予) and `doctrine`
otherwise (e.g. `关于发布《…》的通知`).

## How it works

1. **Document library.** `/ztzl/fgbzwjk/` embeds `/govsearch/haqj.jsp?Stype=2&type=1`.
   Each left-menu category is a `channelid` (from the `getChannel(NNNNN)`
   handlers); listing pages are `&channelid={cid}&page={n}` with 20 rows per
   page, and the embedded `m_nRecordCount` gives the per-channel total.
   Laws/regulations/rules link to MEE detail pages; guides and standards link
   straight to their PDF.
2. **Document listings.** `/zcwj/{path}/` is an `index.html` + `index_{n}.html`
   pager whose page count comes from `createPageHTML(total_pages, …)`.
3. **Full text.** Detail-page bodies are extracted with a balanced-`<div>`
   walk over `Custom_UnionStyle` / `TRS_Editor` (current MEE template),
   `content_body_box` (legacy `/gkml/` pages) and `neiright_JPZGK`
   (NNSA-hosted pages). Instruments are usually promulgated as a short notice
   with the instrument itself attached, so PDF/DOCX attachments are downloaded
   and appended whenever the page body is thin or the title announces a
   document. PDFs go through `common.pdf_extract`, DOCX through stdlib zip+XML.

All channels are drained **round-robin** so that a truncated run — or the
sample — still covers every category and all three data types.

### Known gaps

A handful of pre-2010 guides are typeset with embedded subset CJK fonts whose
text layer decodes to `(cid:NNN)` runs rather than Chinese. Those records are
dropped rather than stored as unusable text (`_is_garbled`); recovering them
would require OCR of the rendered pages.

## Usage

```bash
python bootstrap.py test-api             # connectivity check
python bootstrap.py bootstrap --sample   # ~15 sample documents
python bootstrap.py bootstrap            # full pull
python bootstrap.py bootstrap-fast       # high-throughput full pull (VPS)
```

## Sample data

15 records in `sample/`, 427–131,044 characters of full text, spanning all
three data types (`legislation`, `case_law`, `doctrine`) and every channel
family.

## License

[Government open access — PRC official legal documents](https://www.mee.gov.cn/bzsm/) — laws,
regulations, rules and other official documents of PRC state organs, and their
official translations, are excluded from copyright protection by Article 5 of
the PRC Copyright Law. The MEE/NNSA site statement permits reuse of published
content with attribution to the issuing authority. Commercial use permitted;
attribution required. No explicit open-data licence is published.
