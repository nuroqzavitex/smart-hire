"""TextExtractor backed by PyMuPDF."""

from typing import Any

import pymupdf

from ai_service.services.ports import PdfUnreadable

# PyMuPDF's Document API is untyped; keep the Any boundary in one place.
_open: Any = pymupdf.open


class PyMuPdfExtractor:
    def extract(self, data: bytes) -> str:
        try:
            doc = _open(stream=data, filetype="pdf")
        except Exception as exc:
            # PyMuPDF raises several types (FileDataError, RuntimeError, ...);
            # the message is not propagated as it may quote file content.
            raise PdfUnreadable("pdf cannot be opened") from exc
        try:
            if doc.needs_pass:
                raise PdfUnreadable("pdf is encrypted")
            if doc.page_count == 0:
                raise PdfUnreadable("pdf has no pages")
            try:
                return "".join(str(doc.load_page(i).get_text()) for i in range(doc.page_count))
            except Exception as exc:
                raise PdfUnreadable("pdf cannot be read") from exc
        finally:
            doc.close()
