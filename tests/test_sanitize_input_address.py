from main import sanitize_input_address_raw


def test_sanitize_input_address_multiple_zip_codes_keeps_first() -> None:
    raw = "〒104-8147 東京都中央区銀座2-12-16 A棟 〒104-8148 東京都中央区銀座2-12-18 B棟"
    out = sanitize_input_address_raw(raw)
    assert "〒104-8147" in out
    assert "〒104-8148" not in out


def test_sanitize_input_address_cuts_trailing_noise_markers() -> None:
    raw = "〒381-2281 長野県長野市市場3-48 代表番号"
    out = sanitize_input_address_raw(raw)
    assert out == "〒381-2281 長野県長野市市場3-48"


def test_sanitize_input_address_cuts_google_noise() -> None:
    raw = "〒581-0000 大阪府八尾市恩智1447番地 Google"
    out = sanitize_input_address_raw(raw)
    assert out == "〒581-0000 大阪府八尾市恩智1447番地"


def test_sanitize_input_address_rejects_non_address() -> None:
    assert sanitize_input_address_raw("執行役員 田中 太郎") == ""

