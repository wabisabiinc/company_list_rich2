import re
import unicodedata

KANJI_DIGITS = {
    "零": 0,
    "〇": 0,
    "一": 1,
    "二": 2,
    "三": 3,
    "四": 4,
    "五": 5,
    "六": 6,
    "七": 7,
    "八": 8,
    "九": 9,
}

KANJI_SMALL_UNITS = {
    "十": 10,
    "百": 100,
    "千": 1000,
}

KANJI_LARGE_UNITS = {
    "万": 10_000,
    "億": 100_000_000,
    "兆": 1_000_000_000_000,
}

KANJI_NUMBER_PATTERN = re.compile(r"[零〇一二三四五六七八九十百千万億兆]+")


def _parse_small_section(section: str) -> int | None:
    total = 0
    current = 0
    for ch in section:
        if ch in KANJI_DIGITS:
            current = current * 10 + KANJI_DIGITS[ch]
        elif ch in KANJI_SMALL_UNITS:
            unit = KANJI_SMALL_UNITS[ch]
            if current == 0:
                current = 1
            total += current * unit
            current = 0
        else:
            return None
    return total + current


def kanji_numeral_to_int(text: str) -> int | None:
    if not text:
        return None
    cleaned = unicodedata.normalize("NFKC", text).strip()
    if not cleaned:
        return None
    total = 0
    section = ""
    for ch in cleaned:
        if ch in KANJI_LARGE_UNITS:
            section_value = _parse_small_section(section) if section else 0
            if section_value is None:
                return None
            if section_value == 0:
                section_value = 1
            total += section_value * KANJI_LARGE_UNITS[ch]
            section = ""
        else:
            section += ch
    if section:
        section_value = _parse_small_section(section)
        if section_value is None:
            return None
        total += section_value
    return total if total != 0 else None


def normalize_kanji_numbers(text: str) -> str:
    if not text:
        return ""

    def repl(match: re.Match[str]) -> str:
        value = match.group(0)
        if not any(ch in KANJI_DIGITS for ch in value):
            return value
        converted = kanji_numeral_to_int(value)
        return str(converted) if converted is not None else value

    return KANJI_NUMBER_PATTERN.sub(repl, text)
