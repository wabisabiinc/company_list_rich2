import pytest

from main import normalize_address


def test_normalize_address_strips_contact_info():
    raw = "〒747-0054 東京都大田区羽田空港1-2-3 SKYビル TEL:03-1234-5678 FAX.03-9876-5432"
    assert normalize_address(raw) == "〒747-0054 東京都大田区羽田空港1-2-3 SKYビル"


def test_normalize_address_cuts_map_instructions():
    raw = "〒100-0001 東京都千代田区1-1-1 ビルディング → JR東京駅より徒歩5分 アクセスマップはこちら"
    assert normalize_address(raw) == "〒100-0001 東京都千代田区1-1-1 ビルディング"
