import pytest

from main import normalize_address


def test_normalize_address_strips_contact_info():
    raw = "〒747-0054 東京都大田区羽田空港1-2-3 SKYビル TEL:03-1234-5678 FAX.03-9876-5432"
    assert normalize_address(raw) == "〒747-0054 東京都大田区羽田空港1-2-3 SKYビル"


def test_normalize_address_cuts_map_instructions():
    raw = "〒100-0001 東京都千代田区1-1-1 ビルディング → JR東京駅より徒歩5分 アクセスマップはこちら"
    assert normalize_address(raw) == "〒100-0001 東京都千代田区1-1-1 ビルディング"


def test_normalize_address_rejects_address_form_noise():
    raw = (
        "Japan 郵便番号 (半角数字) 住所検索 都道府県 北海道 青森県 岩手県 宮城県 "
        "秋田県 山形県 福島県 市区町村・番地 マンション・ビル名"
    )
    assert normalize_address(raw) is None


def test_normalize_address_strips_employee_and_permit_tail():
    raw = "〒520-0867 滋賀県大津市大平1丁目3-20 従業員 14名 一般貨物運送許可 可搬運送"
    assert normalize_address(raw) == "〒520-0867 滋賀県大津市大平1丁目3-20"


def test_normalize_address_strips_leading_label():
    raw = "住所：〒671-2232 兵庫県姫路市御国野町御着86-6"
    assert normalize_address(raw) == "〒671-2232 兵庫県姫路市御国野町御着86-6"
