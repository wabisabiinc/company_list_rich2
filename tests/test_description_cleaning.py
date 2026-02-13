import main


def test_clean_description_accepts_wholesale_keyword() -> None:
    text = "医療機器の卸売・販売を行う企業です。"
    assert main.clean_description_value(text)


def test_build_final_description_from_payloads_rebuilds_from_text() -> None:
    payloads = [
        {
            "text": "当社は建設工事の設計・施工を行う企業です。お問い合わせはこちら。",
            "html": "",
        }
    ]
    desc = main.build_final_description_from_payloads(payloads, min_len=20, max_len=160)
    assert isinstance(desc, str) and desc


def test_ensure_name_industry_in_description_inserts_industry_when_missing() -> None:
    src = "株式会社Exampleは、法人向けソフトウェアを開発・提供しています。"
    out = main._ensure_name_industry_in_description(src, "株式会社Example", "IT・ソフトウェア")
    assert "株式会社Example（IT・ソフトウェア）" in out


def test_ensure_name_industry_in_description_keeps_text_when_industry_unknown() -> None:
    src = "株式会社Exampleは、法人向けソフトウェアを開発・提供しています。"
    out = main._ensure_name_industry_in_description(src, "株式会社Example", "不明")
    assert out == src
