# VN/ThuVienPhapLuat — Thu Vien Phap Luat (Legal Library)

**Source:** [https://thuvienphapluat.vn/en/](https://thuvienphapluat.vn/en/)
**Access route:** HuggingFace dataset [`th1nhng0/vietnamese-legal-documents`](https://huggingface.co/datasets/th1nhng0/vietnamese-legal-documents)
**Data types:** legislation

## Access

thuvienphapluat.vn itself returns HTTP 403 to non-Vietnamese clients, so the corpus is
read from the maintainer's HuggingFace mirror. Two configs are joined on the string `id`:

| Config | Split | Rows | Columns |
|--------|-------|------|---------|
| `metadata` | `data` | 171,556 | `id`, `title`, `so_ky_hieu`, `ngay_ban_hanh`, `loai_van_ban`, `co_quan_ban_hanh`, `nguoi_ky`, … (Vietnamese names) |
| `content` | `data` | 170,824 | `id`, `content_html` |

`content_html` is stripped to plain text with a dependency-free regex extractor.

Notes for maintainers:

- **Ids are strings, and ~9% are non-numeric** (e.g. `vbpqta_2709`). The on-disk SQLite
  metadata cache must declare `id TEXT PRIMARY KEY` — an `INTEGER PRIMARY KEY` raises
  `sqlite3.IntegrityError: datatype mismatch` mid-stream (issue #1297).
- The `legacy_metadata` / `legacy_content` configs are the pre-restructure dumps;
  `legacy_content` currently returns HTTP 500 from datasets-server and is not used.
- The `metadata` and `content` configs are **not row-aligned**, so sampling looks metadata
  up per id through the datasets-server `filter` endpoint rather than by matching offsets.

## Commands

```bash
python3 bootstrap.py bootstrap --sample   # 15 validation samples
python3 bootstrap.py bootstrap-fast       # full corpus -> data/records.jsonl
```

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — the HuggingFace dataset is
published under CC BY 4.0; attribution required. Commercial use permitted.
