import pytest
import requests

from src.company_scraper import CompanyScraper


class _FakeResp:
    def __init__(self, url: str, content: bytes, status_code: int = 200):
        self.url = url
        self.content = content
        self.status_code = status_code
        self.headers = {"Content-Type": "text/html; charset=utf-8"}
        self.apparent_encoding = "utf-8"

    def raise_for_status(self):
        if int(self.status_code) >= 400:
            raise requests.HTTPError(f"status={self.status_code}")


@pytest.mark.asyncio
async def test_fetch_http_info_fallbacks_to_http_on_ssl_error(monkeypatch):
    scraper = CompanyScraper(headless=True)

    def fake_get(url, *args, **kwargs):
        if url.startswith("https://"):
            raise requests.exceptions.SSLError("hostname mismatch")
        return _FakeResp(url, b"<html><body>OK</body></html>")

    monkeypatch.setattr(scraper, "_session_get", fake_get)

    info = await scraper._fetch_http_info("https://example.co.jp/company/", timeout_ms=4000)  # type: ignore[attr-defined]
    assert (info.get("text") or "").strip()
    assert (info.get("html") or "").strip()
