from main import pick_best_phone, pick_best_phone_from_entries
from src.company_scraper import CompanyScraper


def test_pick_best_phone_prefers_hq_over_branch_even_if_structured_exists():
    # structured(TABLE) があっても、footer 由来が「本社/代表」文脈なら捨てない
    candidates = [
        "[TABLE][BRANCH]03-1111-2222",
        "[FOOTER][HQ]03-3333-4444",
    ]
    assert pick_best_phone(candidates) == "03-3333-4444"


def test_extract_candidates_labels_add_context_tags_and_pick_prefers_keiri():
    scraper = CompanyScraper(headless=True)
    html = """
    <html><body>
      <table>
        <tr><th>電話番号</th><td>03-0000-0000</td></tr>
        <tr><th>経理部</th><td>03-2222-3333</td></tr>
      </table>
    </body></html>
    """
    cc = scraper.extract_candidates(text="", html=html)
    phones = cc.get("phone_numbers") or []
    assert any("[KEIRI]" in p for p in phones)
    assert pick_best_phone(phones) == "03-2222-3333"


def test_extract_candidates_text_context_tags_hq():
    scraper = CompanyScraper(headless=True)
    text = "本社 代表電話: 03-4444-5555"
    cc = scraper.extract_candidates(text=text, html=None)
    phones = cc.get("phone_numbers") or []
    assert any("[HQ]" in p or "[REP]" in p for p in phones)
    assert pick_best_phone(phones) == "03-4444-5555"


def test_pick_best_phone_from_entries_prefers_hq_from_footer_over_branch_table():
    entries = [
        ("[TABLE][BRANCH]03-1111-2222", "https://example.com/profile", "COMPANY_PROFILE"),
        ("[FOOTER][HQ]03-3333-4444", "https://example.com/", "OTHER"),
    ]
    assert pick_best_phone_from_entries(entries) == ("03-3333-4444", "https://example.com/")


def test_pick_best_phone_from_entries_rejects_weak_other():
    entries = [
        ("03-1111-2222", "https://example.com/a", "OTHER"),
        ("[TABLE]03-3333-4444", "https://example.com/b", "OTHER"),
    ]
    assert pick_best_phone_from_entries(entries) == ("03-3333-4444", "https://example.com/b")
