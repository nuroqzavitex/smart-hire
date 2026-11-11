"""PDF fixtures for tests, generated with PyMuPDF (plan decision #14)."""

import pymupdf


def pdf(lines: list[str]) -> bytes:
    """A PDF whose text layer holds the given lines (new page every 60 lines)."""
    doc = pymupdf.open()
    for start in range(0, max(len(lines), 1), 60):
        page = doc.new_page()
        chunk = lines[start : start + 60]
        if chunk:
            page.insert_text((36, 36), "\n".join(chunk), fontsize=8)
    data: bytes = doc.tobytes()
    doc.close()
    return data


def image_only_pdf() -> bytes:
    """A one-page PDF containing only an image (no text layer)."""
    doc = pymupdf.open()
    page = doc.new_page()
    pix = pymupdf.Pixmap(pymupdf.csRGB, pymupdf.IRect(0, 0, 32, 32), False)
    pix.clear_with(200)
    page.insert_image(pymupdf.Rect(36, 36, 236, 236), pixmap=pix)
    data: bytes = doc.tobytes()
    doc.close()
    return data


def encrypted_pdf(password: str) -> bytes:
    """A text PDF that needs a user password to open."""
    doc = pymupdf.open(stream=pdf(["Encrypted Candidate", "secret@example.com"] + ["z" * 80] * 3))
    data: bytes = doc.tobytes(
        encryption=pymupdf.PDF_ENCRYPT_AES_256, user_pw=password, owner_pw=password + "-owner"
    )
    doc.close()
    return data
