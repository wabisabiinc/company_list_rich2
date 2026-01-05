import pytest

pytest.importorskip("pypdf")

from src.company_scraper import CompanyScraper


def _build_minimal_pdf(text: str) -> bytes:
    # Minimal one-page PDF with Helvetica text (ASCII only).
    # Build objects and xref offsets programmatically.
    def obj(n: int, body: bytes) -> bytes:
        return f"{n} 0 obj\n".encode() + body + b"\nendobj\n"

    safe = text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
    content = f"BT /F1 12 Tf 10 100 Td ({safe}) Tj ET".encode()

    parts: list[bytes] = []
    parts.append(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
    offsets = [0]

    def add(b: bytes) -> None:
        offsets.append(sum(len(x) for x in parts))
        parts.append(b)

    add(obj(1, b"<< /Type /Catalog /Pages 2 0 R >>"))
    add(obj(2, b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>"))
    add(
        obj(
            3,
            b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 300 144] "
            b"/Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>",
        )
    )
    add(obj(4, b"<< /Length " + str(len(content)).encode() + b" >>\nstream\n" + content + b"\nendstream"))
    add(obj(5, b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>"))

    xref_start = sum(len(x) for x in parts)
    count = 6  # obj 0..5
    xref = [b"xref\n0 %d\n" % count]
    xref.append(b"0000000000 65535 f \n")
    for i in range(1, count):
        off = offsets[i]
        xref.append(f"{off:010d} 00000 n \n".encode())
    trailer = (
        b"trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n" % (count, xref_start)
    )
    return b"".join(parts) + b"".join(xref) + trailer


def test_extract_pdf_text_returns_embedded_ascii_text() -> None:
    raw = _build_minimal_pdf("COMPANY PROFILE CEO John Smith")
    extracted = CompanyScraper._extract_pdf_text(raw)
    assert "COMPANY" in extracted
    assert "John" in extracted
