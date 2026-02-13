from __future__ import annotations

import re
import unicodedata
from typing import Any

_SYMBOL_RE = re.compile(r"[／/・,，.．\-‐‑‒–—―ー_()（）\[\]{}<>＜＞「」『』【】〈〉《》\"'`´･]+")
_SPACE_RE = re.compile(r"\s+")


def norm_text(value: Any) -> str:
    """
    Normalize text for rule-based classification.
    - NFKC
    - lowercase
    - remove punctuation/symbols used as separators
    - compress whitespaces
    """
    text = unicodedata.normalize("NFKC", str(value or ""))
    text = text.lower().replace("\u3000", " ")
    text = _SYMBOL_RE.sub(" ", text)
    text = _SPACE_RE.sub(" ", text).strip()
    return text


def norm_text_compact(value: Any) -> str:
    return norm_text(value).replace(" ", "")
