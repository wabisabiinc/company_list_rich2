import pytest

import main


class _DummyScraper:
    async def _fetch_http_info(self, url: str, timeout_ms: int = 0, allow_slow: bool = False):
        return {"url": url, "html": "<html><body>same</body></html>", "text": "same"}

    def compute_homepage_fingerprint(self, html: str, text: str) -> str:
        return "fp_same"


class _DummyManager:
    def __init__(self):
        self.saved = []

    def save_update_check_result(self, **kwargs):
        self.saved.append(kwargs)


def test_pick_update_check_url_prefers_final_homepage():
    company = {
        "final_homepage": "https://final.example.com",
        "homepage": "https://homepage.example.com",
        "homepage_official_flag": 1,
    }
    assert main._pick_update_check_url(company) == ("https://final.example.com", "final_homepage")


@pytest.mark.asyncio
async def test_maybe_skip_if_unchanged_forces_recrawl_when_logic_hash_changed(monkeypatch):
    monkeypatch.setattr(main, "UPDATE_CHECK_ENABLED", True)
    monkeypatch.setattr(main, "CURRENT_UPDATE_CHECK_LOGIC_HASH", "auto:new")

    company = {
        "id": 1,
        "final_homepage": "https://example.com",
        "homepage_check_url": "https://example.com",
        "homepage_fingerprint": "fp_same",
        "homepage_check_logic_hash": "auto:old",
    }
    scraper = _DummyScraper()
    manager = _DummyManager()

    skipped = await main.maybe_skip_if_unchanged(company, scraper, manager)

    assert skipped is False
    assert company.get("homepage_check_logic_hash") == "auto:new"
    assert manager.saved == []


@pytest.mark.asyncio
async def test_maybe_skip_if_unchanged_skips_when_logic_hash_is_same(monkeypatch):
    monkeypatch.setattr(main, "UPDATE_CHECK_ENABLED", True)
    monkeypatch.setattr(main, "CURRENT_UPDATE_CHECK_LOGIC_HASH", "auto:same")

    company = {
        "id": 2,
        "final_homepage": "https://example.com",
        "homepage_check_url": "https://example.com",
        "homepage_fingerprint": "fp_same",
        "homepage_check_logic_hash": "auto:same",
    }
    scraper = _DummyScraper()
    manager = _DummyManager()

    skipped = await main.maybe_skip_if_unchanged(company, scraper, manager)

    assert skipped is True
    assert len(manager.saved) == 1
    assert manager.saved[0]["homepage_check_logic_hash"] == "auto:same"
    assert manager.saved[0]["status"] == "done"

