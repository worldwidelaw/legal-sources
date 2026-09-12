# KR/NSSC — Nuclear Safety and Security Commission (원자력안전위원회)

Korea's independent nuclear regulator. The commission licenses nuclear reactors
and radiation facilities, sets the binding safety standards under the Nuclear
Safety Act (원자력안전법), and imposes administrative sanctions on licensees.

- **Site:** https://www.nssc.go.kr/
- **Country:** KR
- **Data types:** `legislation`, `doctrine`
- **Language:** Korean (`ko`)
- **Auth:** none

## Coverage

| Board | `BOARD_SEQ` | Content | `_type` |
|---|---|---|---|
| 최근개정법령 | 39 | Promulgation text of every amendment to the nuclear-safety stack — 원자력안전법, its Enforcement Decree and Enforcement Rules, and the 원자력안전위원회고시 carrying the operative technical requirements (radiation protection, reactor design and operation, transport and packaging, radioactive-waste management), each with supplementary provisions and statement of reasons | `legislation` |
| 일정/회의결과/회의록 | 14 | Agenda and disposition of every commission sitting, plus the record of decisions (의사록) and the verbatim transcript (회의록) — the only public record of the commission's reasoning on licence grants, licence amendments and enforcement action | `doctrine` |

## Access

The rendered board pages are empty shells; every row is delivered by JSON
endpoints:

- `POST /ajaxf/FR_BBS_SVC/BBSViewList.do` — listing rows, already including the
  board body in `CONTENTS`. `pagePerCnt` is ignored (always 15 rows), so paging
  walks `pageNo` against `totalRecordCount`.
- `POST /ajaxf/FR_BBS_SVC/BBSViewAttachList.do` — attachment manifest per posting.
- `GET /ajaxfile/FR_SVC/FileDown.do` — attachment bytes.

Full text is the board body concatenated with text extracted from the attached
Korean PDF / HWP / HWPX / DOC documents (`common.pdf_extract`,
`common.hwp_extract`, `common.doc_extract`).

## Usage

```bash
python sources/KR/NSSC/bootstrap.py bootstrap --sample   # sample records
python sources/KR/NSSC/bootstrap.py bootstrap --full     # full corpus
python sources/KR/NSSC/bootstrap.py bootstrap-fast       # high-throughput full pull (VPS)
```

## Record shape

```json
{
  "_id": "nssc-39-12345",
  "_source": "KR/NSSC",
  "_type": "legislation",
  "_fetched_at": "2026-08-06T17:30:38+00:00",
  "title": "방사선방호 등에 관한 기준",
  "text": "◇ 개정이유 및 주요내용 ...",
  "date": "2026-07-07",
  "url": "https://www.nssc.go.kr/...",
  "language": "ko",
  "country": "KR",
  "authority": "원자력안전위원회",
  "document_type": "고시",
  "instrument_type": "고시",
  "instrument_number": "원자력안전위원회고시 제2026-12호"
}
```

## License

[KOGL Type 1 — 공공누리 제1유형](https://www.kogl.or.kr/info/license.do) —
commercial use, derivative works and redistribution are permitted; attribution
to 원자력안전위원회 with the source URL is required.

The NSSC copyright policy (`MENU_ID=2580`) states that works whose economic
rights it holds in full may be used without separate permission under article
24-2 of the Korean Copyright Act (free use of public works), and are released
under KOGL Type 1. The promulgated statutory texts collected here are in any
case outside copyright under article 7 of the Copyright Act, which excludes
법령 and official notices from protection.
