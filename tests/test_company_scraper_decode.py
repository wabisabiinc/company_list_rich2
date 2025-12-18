import types

from src.company_scraper import CompanyScraper


def _dummy_response(apparent: str | None = None):
    resp = types.SimpleNamespace()
    resp.apparent_encoding = apparent
    return resp


def test_detect_html_encoding_prefers_meta_charset():
    raw = b'<html><head><meta charset="UTF-8"></head><body></body></html>'
    resp = _dummy_response(apparent="shift_jis")
    enc = CompanyScraper._detect_html_encoding(resp, raw)
    assert enc.lower() == "utf-8"


def test_detect_html_encoding_falls_back_to_apparent():
    raw = b"<html><head></head><body></body></html>"
    resp = _dummy_response(apparent="shift_jis")
    enc = CompanyScraper._detect_html_encoding(resp, raw)
    assert enc.lower() == "shift_jis"


def test_detect_html_encoding_default_utf8():
    raw = b""
    resp = _dummy_response(apparent=None)
    enc = CompanyScraper._detect_html_encoding(resp, raw)
    assert enc.lower() == "utf-8"
