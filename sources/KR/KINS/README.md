# KR/KINS — Korea Institute of Nuclear Safety (한국원자력안전기술원)

Full text of the technical safety standards and regulatory guides published by
KINS, the statutory technical support organisation of Korea's **Nuclear Safety
and Security Commission (NSSC)**.

The Nuclear Safety Act (원자력안전법), its Enforcement Decree and Rules and the
NSSC notices (고시) state *what* a nuclear installation must achieve. The KINS
regulatory guides state the methods and acceptance criteria the regulator
treats as satisfying those requirements — they are what a licensee's safety
analysis report is actually reviewed against, and are the Korean analogue of
the US NRC Regulatory Guide series.

## Coverage

| Stream | What it is | Size |
|---|---|---|
| **KINS 규제지침** (regulatory guides) | Light-water-reactor guides organised as a chapter tree: site, radiological environment, reactor, design, materials, I&C, electrical systems, fuel, accident analysis, initial testing, technical specifications, radiation protection, waste, emergency preparedness, quality assurance | ~210 guides |
| **KINS 규제기준** (regulatory standards) | The older standards series, being wound down — the 2016-17 consistency review and 2018 repeal-and-transfer plan folded almost every chapter into the guides, so only chapters still awaiting transfer remain live | whatever the tree reports |
| **NuSSAM 공지사항** (notices) | Official announcements of enactment, amendment and repeal, and the public consultations on draft guides — the only place the *reasons* for a change are stated | 33 posts |

**Every revision is kept.** Each guide carries its full history — 제정
(enactment), 개정 (amendment), 폐지 (repeal) — and each revision is a separate
record, so the version in force on a given date can be recovered. Roughly 580
revision PDFs across the guide tree.

All records are typed `doctrine`: KINS guidance interprets and supplements
binding law rather than being binding law itself. The binding layer is
deliberately **not** duplicated here — NuSSAM links out to law.go.kr for it and
`KR/LawGoKr` already carries it.

## Access

Everything is served from **NuSSAM** (원자력안전기준관리시스템, the nuclear safety
standards management system) at `www.kins.re.kr/nussam`.

| Endpoint | Method | Purpose |
|---|---|---|
| `/krs/KinsRgltManualReresvnSts.do` | GET | Guide tree — ships inline as JSON in the `#treeStringValue` hidden input (not paginated) |
| `/krs/KinsRgltBassReresvnSts.do` | GET | Standards tree, same shape |
| `/krs/getAjaxLawDetlList.do` | POST | Revision history for one document, as an HTML `<tr>` table in a JSON envelope |
| `/common/NussamFileDownLoad.do` | POST | The PDF, keyed by the opaque stored filename |
| `/board/NotiMtrList.do` · `/board/NotiMtrDetl.do` | POST | Notices (paged with `currentPage`) |

Gotchas worth knowing before touching this scraper:

- `kins.re.kr` answers **only on the `www` host** — the apex has no A record —
  and resets connections from non-browser User-Agents. The session sends a
  desktop Chrome UA.
- `NussamFileDownLoad.do` is **POST-only**; a GET returns HTTP 405. There is
  therefore no per-document permalink, so `url` points at the listing page and
  the stored/original filenames are carried in `file_name`.
- Board bodies are **double HTML-escaped** (spaces as `&amp;nbsp;`, punctuation as
  `&amp;#47;`), so `strip_tags()` unescapes twice.
- PDFs are born-digital Korean; text extracts cleanly with pdfplumber/pypdf, no
  OCR needed.

## Usage

```bash
python bootstrap.py test-api             # Connectivity + full-text check
python bootstrap.py bootstrap --sample   # ~12 sample documents
python bootstrap.py bootstrap            # Full pull
python bootstrap.py bootstrap-fast       # High-throughput full pull (VPS)
```

## Sample record

```json
{
  "_id": "regulatory_guide-187-r3",
  "_source": "KR/KINS",
  "_type": "doctrine",
  "title": "KINS 규제지침 2.1 원자로시설 부지주변 해양특성 조사 (Rev. 3)",
  "date": "2023-01-19",
  "code": "KINS/RG-N02.01",
  "guide_number": "2.1",
  "revision": "3",
  "change_type": "amendment",
  "chapter_path": ["규제지침", "02장 방사선환경"],
  "text": "2.1 원자로시설 부지주변 해양특성 조사 ❚ KINS/RG-N02.01, Rev. 2 …"
}
```

## License

[Public data under the Public Data Act / Copyright Act art. 24-2](https://www.kins.re.kr/copyright)
— attribution required.

KINS's copyright policy states that works whose economic rights are held in
full by KINS may be used freely without separate permission under article 24-2
of the Korean Copyright Act (자유이용 of public works), and that the public data
the site provides may be used by anyone "including for commercial purposes"
(영리 목적의 이용을 포함한 자유로운 활용이 보장됩니다) under the Public Data Act. The
regulatory guides, standards and NuSSAM notices collected here are authored in
full by KINS and fall in that class. The policy excludes material KINS does not
wholly own; this scraper collects nothing outside the KINS-authored series.
