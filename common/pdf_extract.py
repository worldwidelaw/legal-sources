"""
Shared PDF extraction helpers for Legal Data Hunter scrapers.

The scrapers call extract_pdf_markdown with a few historical signatures:

  - extract_pdf_markdown(pdf_path)
  - extract_pdf_markdown(pdf_bytes)
  - extract_pdf_markdown("SOURCE/ID", doc_id, pdf_bytes=...)
  - extract_pdf_markdown(source="SOURCE/ID", source_id=..., pdf_url=...)

This module keeps those call styles working and falls back cleanly when the
optional Neon database or PDF extraction libraries are unavailable.
"""

from __future__ import annotations

import io
import logging
import os
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger("legal-data-hunter.pdf_extract")


def _looks_like_pdf(data: bytes) -> bool:
    return data.lstrip().startswith(b"%PDF")


def _download_pdf(url: str, timeout: int = 120) -> Optional[bytes]:
    try:
        import requests

        response = requests.get(
            url,
            timeout=timeout,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "application/pdf,*/*;q=0.8",
            },
        )
        response.raise_for_status()
        return response.content
    except Exception as exc:
        logger.warning("PDF download failed for %s: %s", url, exc)
        return None


def _coerce_pdf_bytes(pdf_input: Any = None, pdf_bytes: Any = None, pdf_url: str | None = None) -> Optional[bytes]:
    if pdf_bytes is not None:
        if isinstance(pdf_bytes, bytes):
            return pdf_bytes
        if isinstance(pdf_bytes, bytearray):
            return bytes(pdf_bytes)
        if hasattr(pdf_bytes, "read"):
            return pdf_bytes.read()

    if pdf_url:
        return _download_pdf(pdf_url)

    if pdf_input is None:
        return None

    if isinstance(pdf_input, bytes):
        return pdf_input
    if isinstance(pdf_input, bytearray):
        return bytes(pdf_input)
    if hasattr(pdf_input, "read"):
        return pdf_input.read()

    if isinstance(pdf_input, (str, os.PathLike)):
        value = str(pdf_input)
        if value.startswith(("http://", "https://")):
            return _download_pdf(value)

        path = Path(value)
        if path.exists() and path.is_file():
            return path.read_bytes()

    return None


def _extract_with_pdfplumber(pdf_bytes: bytes, max_pages: int | None = None) -> Optional[str]:
    try:
        import pdfplumber
    except Exception:
        return None

    try:
        parts: list[str] = []
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            pages = pdf.pages[:max_pages] if max_pages else pdf.pages
            for page in pages:
                text = page.extract_text() or ""
                if text.strip():
                    parts.append(text.strip())
        text = "\n\n".join(parts).strip()
        return text or None
    except Exception as exc:
        logger.debug("pdfplumber extraction failed: %s", exc)
        return None


def _extract_with_pypdf(pdf_bytes: bytes, max_pages: int | None = None) -> Optional[str]:
    try:
        from pypdf import PdfReader
    except Exception:
        return None

    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        pages = reader.pages[:max_pages] if max_pages else reader.pages
        parts = []
        for page in pages:
            text = page.extract_text() or ""
            if text.strip():
                parts.append(text.strip())
        text = "\n\n".join(parts).strip()
        return text or None
    except Exception as exc:
        logger.debug("pypdf extraction failed: %s", exc)
        return None


def extract_pdf_markdown(
    *args: Any,
    source: str | None = None,
    source_id: str | None = None,
    pdf_bytes: Any = None,
    pdf_url: str | None = None,
    table: str | None = None,
    max_pages: int | None = None,
    **_: Any,
) -> str:
    """
    Extract text from a PDF and return markdown-compatible plain text.

    The source, source_id, and table parameters are accepted for compatibility
    with source scrapers and future telemetry, but extraction works without
    them.
    """

    pdf_input = None

    if args:
        if pdf_bytes is None and pdf_url is None and len(args) == 1:
            pdf_input = args[0]
        else:
            source = source or str(args[0])
            if len(args) > 1:
                source_id = source_id or str(args[1])

    data = _coerce_pdf_bytes(pdf_input=pdf_input, pdf_bytes=pdf_bytes, pdf_url=pdf_url)
    if not data:
        logger.warning("No PDF bytes supplied for %s/%s", source or "unknown", source_id or "unknown")
        return ""

    if not _looks_like_pdf(data):
        logger.warning("Content does not look like a PDF for %s/%s", source or "unknown", source_id or "unknown")

    for extractor in (_extract_with_pdfplumber, _extract_with_pypdf):
        text = extractor(data, max_pages=max_pages)
        if text:
            return text

    logger.warning("PDF text extraction returned no text for %s/%s", source or "unknown", source_id or "unknown")
    return ""


def preload_existing_ids(source: str, table: str = "doctrine") -> set[str]:
    """
    Return IDs already present in Neon for a source.

    When NEON_DATABASE_URL is not configured, or the database schema is not
    reachable from the current host, this intentionally returns an empty set so
    scrapers remain runnable in local and CI environments.
    """

    database_url = os.environ.get("NEON_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not database_url:
        return set()

    safe_tables = {"case_law", "doctrine", "legislation"}
    if table not in safe_tables:
        logger.warning("Refusing to preload IDs from unexpected table: %s", table)
        return set()

    try:
        import psycopg2
        from psycopg2 import sql

        with psycopg2.connect(database_url) as conn:
            with conn.cursor() as cur:
                query = sql.SQL(
                    """
                    SELECT _id
                    FROM {table}
                    WHERE _source = %s
                      AND COALESCE(text, '') <> ''
                    """
                ).format(table=sql.Identifier(table))
                cur.execute(query, (source,))
                return {str(row[0]) for row in cur.fetchall() if row and row[0]}
    except Exception as exc:
        logger.warning("Could not preload existing IDs for %s/%s: %s", table, source, exc)
        return set()
