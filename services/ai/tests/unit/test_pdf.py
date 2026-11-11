import pytest
from pdfs import encrypted_pdf, image_only_pdf, pdf

from ai_service.adapters import pdf as pdf_module
from ai_service.adapters.pdf import PyMuPdfExtractor
from ai_service.services.ports import PdfUnreadable


def test_extracts_text_from_all_pages() -> None:
    lines = [f"line {i}" for i in range(130)]  # 3 pages
    text = PyMuPdfExtractor().extract(pdf(lines))
    assert "line 0" in text
    assert "line 65" in text
    assert "line 129" in text


def test_image_only_pdf_has_no_text() -> None:
    assert PyMuPdfExtractor().extract(image_only_pdf()).strip() == ""


# Non-PDF bytes (e.g. HTML) are rejected by parse_cv's %PDF- magic check before
# extraction; MuPDF would "repair" them into a blank page.
@pytest.mark.parametrize(
    "data",
    [b"", b"%PDF-1.4 garbage", encrypted_pdf("secret")],
    ids=["empty", "corrupt", "encrypted"],
)
def test_unreadable_raises(data: bytes) -> None:
    with pytest.raises(PdfUnreadable):
        PyMuPdfExtractor().extract(data)


ZERO_PAGE_PDF = (
    b"%PDF-1.4\n1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
    b"2 0 obj<</Type/Pages/Kids[]/Count 0>>endobj\ntrailer<</Root 1 0 R>>\n%%EOF\n"
)


def test_zero_page_pdf_is_unreadable() -> None:
    with pytest.raises(PdfUnreadable, match="no pages"):
        PyMuPdfExtractor().extract(ZERO_PAGE_PDF)


class _BrokenPage:
    def get_text(self) -> str:
        raise RuntimeError("mupdf: content stream error near 'Nguyen Van A'")


class _BrokenDoc:
    needs_pass = False
    page_count = 1
    closed = False

    def load_page(self, _: int) -> _BrokenPage:
        return _BrokenPage()

    def close(self) -> None:
        self.closed = True


def test_get_text_error_is_unreadable_without_leaking_message(monkeypatch: pytest.MonkeyPatch) -> None:
    doc = _BrokenDoc()
    monkeypatch.setattr(pdf_module, "_open", lambda **_: doc)
    with pytest.raises(PdfUnreadable) as exc:
        PyMuPdfExtractor().extract(pdf(["anything"]))
    assert exc.value.args == ("pdf cannot be read",)
    assert doc.closed
