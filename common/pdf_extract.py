import io
import logging

logger = logging.getLogger("legal-data-hunter")


def extract_pdf_markdown(pdf_bytes: bytes) -> str:
    try:
        import pdfplumber
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            pages = []
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    pages.append(text)
            return "\n\n".join(pages)
    except Exception as e:
        logger.warning(f"pdfplumber failed: {e}, trying pypdf")
        try:
            import pypdf
            reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
            pages = []
            for page in reader.pages:
                text = page.extract_text()
                if text:
                    pages.append(text)
            return "\n\n".join(pages)
        except Exception as e2:
            logger.error(f"pypdf also failed: {e2}")
            return ""


def preload_existing_ids(source_id: str) -> set:
    from pathlib import Path
    import json

    ids = set()
    sample_dir = Path("sources") / source_id / "sample"
    if sample_dir.exists():
        for f in sample_dir.glob("*.json"):
            try:
                data = json.loads(f.read_text())
                if "_id" in data:
                    ids.add(data["_id"])
            except Exception:
                pass
    return ids
