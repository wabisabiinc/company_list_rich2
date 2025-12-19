import re
import unicodedata
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Iterable, Optional

from bs4 import BeautifulSoup


_CORP_SUFFIXES = (
    "株式会社",
    "有限会社",
    "合同会社",
    "合資会社",
    "合名会社",
    "一般社団法人",
    "一般財団法人",
    "公益社団法人",
    "公益財団法人",
    "医療法人",
    "学校法人",
    "社会福祉法人",
    "宗教法人",
    "特定非営利活動法人",
    "NPO法人",
    "独立行政法人",
    "(株)",
    "（株）",
    "㈱",
    "(有)",
    "（有）",
    "㈲",
    "(同)",
    "（同）",
)

_TITLE_SPLIT_RE = re.compile(r"\s*(?://|\||｜|/| - | – | — |:|：)\s*")
_WS_RE = re.compile(r"\s+")
_PUNCT_STRIP_RE = re.compile(r"[　\\s\\-‐―－ー–—\\|｜/:：,，.。・･…\\(\\)（）\\[\\]【】<>＜＞]+")


def normalize_company_name(name: str) -> str:
    if not name:
        return ""
    normalized = unicodedata.normalize("NFKC", name)
    for suf in _CORP_SUFFIXES:
        normalized = normalized.replace(suf, "")
    normalized = _WS_RE.sub("", normalized)
    normalized = _PUNCT_STRIP_RE.sub("", normalized)
    return normalized


def normalize_site_name(text: str) -> str:
    if not text:
        return ""
    normalized = unicodedata.normalize("NFKC", text)
    normalized = normalized.strip()
    for suf in _CORP_SUFFIXES:
        normalized = normalized.replace(suf, "")
    normalized = _WS_RE.sub("", normalized)
    normalized = _PUNCT_STRIP_RE.sub("", normalized)
    return normalized


def _split_title_like(text: str) -> list[str]:
    t = unicodedata.normalize("NFKC", text or "").strip()
    if not t:
        return []
    parts: list[str] = []
    for part in _TITLE_SPLIT_RE.split(t):
        p = part.strip()
        if p:
            parts.append(p)
    return parts or [t]


def extract_name_signals(html: str, text: str) -> dict[str, str]:
    signals: dict[str, str] = {}
    if html:
        try:
            soup = BeautifulSoup(html, "html.parser")
        except Exception:
            soup = None
        if soup:
            title = soup.title.string if soup.title and soup.title.string else ""
            if title:
                signals["title"] = title.strip()
            h1 = soup.find("h1")
            if h1:
                h1_text = h1.get_text(separator=" ", strip=True)
                if h1_text:
                    signals["h1"] = h1_text
            for attr in ("og:site_name", "og:title", "application-name"):
                node = soup.find("meta", attrs={"property": attr}) or soup.find("meta", attrs={"name": attr})
                if node:
                    content = node.get("content")
                    if content:
                        key = "og_site_name" if attr == "og:site_name" else ("og_title" if attr == "og:title" else "app_name")
                        signals[key] = str(content).strip()

    if text:
        head = unicodedata.normalize("NFKC", text)
        head = _WS_RE.sub(" ", head).strip()
        if head:
            signals["body_head"] = head[:240]
    return signals


@dataclass(frozen=True)
class NameMatchResult:
    company_norm: str
    best_candidate_raw: str
    best_candidate_norm: str
    best_source: str
    ratio: float
    exact: bool
    partial_only: bool


def _iter_candidates(signals: dict[str, str]) -> Iterable[tuple[str, str]]:
    for source, raw in signals.items():
        if not raw:
            continue
        for part in _split_title_like(raw):
            yield source, part


def score_name_match(company_name: str, signals: dict[str, str]) -> NameMatchResult:
    company_norm = normalize_company_name(company_name)
    best_source = ""
    best_raw = ""
    best_norm = ""
    best_ratio = 0.0
    best_exact = False
    best_partial = False

    for source, cand in _iter_candidates(signals):
        cand_norm = normalize_site_name(cand)
        if not cand_norm:
            continue
        if not company_norm:
            continue
        ratio = SequenceMatcher(None, company_norm, cand_norm).ratio()
        exact = company_norm == cand_norm
        partial = (company_norm in cand_norm or cand_norm in company_norm) and not exact

        # 「部分一致しかしていない」ケースの過大評価を抑える
        if partial and len(company_norm) >= 3 and len(cand_norm) >= 3:
            ratio = min(ratio, 0.69)

        if exact:
            ratio = 1.0
        if ratio > best_ratio:
            best_ratio = ratio
            best_source = source
            best_raw = cand
            best_norm = cand_norm
            best_exact = exact
            best_partial = partial

    # company_norm が極端に短いと誤一致しやすいので、部分一致は強く抑制
    if company_norm and len(company_norm) <= 2 and best_partial and not best_exact:
        best_ratio = min(best_ratio, 0.4)

    return NameMatchResult(
        company_norm=company_norm,
        best_candidate_raw=best_raw,
        best_candidate_norm=best_norm,
        best_source=best_source,
        ratio=float(best_ratio),
        exact=bool(best_exact),
        partial_only=bool(best_partial and not best_exact),
    )
