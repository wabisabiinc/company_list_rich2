from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class HomepageDecision:
    homepage: str
    homepage_official_flag: int
    homepage_official_source: str
    homepage_official_score: float
    chosen_domain_score: int
    dropped: bool


def apply_provisional_homepage_policy(
    *,
    homepage: str,
    homepage_official_flag: int,
    homepage_official_source: str,
    homepage_official_score: float,
    chosen_domain_score: int,
    provisional_host_token: bool,
    provisional_name_present: bool,
    provisional_address_ok: bool,
    provisional_ai_hint: bool = False,
    provisional_profile_hit: bool = False,
    provisional_evidence_score: int = 0,
) -> HomepageDecision:
    """
    暫定URL（provisional_*）のうち「弱すぎるものだけ」を homepage から外すための判定。

    注意:
    - URL自体の記録（provisional_homepage / final_homepage）までは扱わない（呼び出し側の責務）。
    - homepage_official_source が "provisional" で始まるものだけ対象。
    """
    if not homepage:
        return HomepageDecision(
            homepage="",
            homepage_official_flag=int(homepage_official_flag or 0),
            homepage_official_source=str(homepage_official_source or ""),
            homepage_official_score=float(homepage_official_score or 0.0),
            chosen_domain_score=int(chosen_domain_score or 0),
            dropped=False,
        )

    source = (homepage_official_source or "").strip()
    if int(homepage_official_flag or 0) != 0 or not (
        source.startswith("provisional") or source.startswith("ai_provisional")
    ):
        return HomepageDecision(
            homepage=homepage,
            homepage_official_flag=int(homepage_official_flag or 0),
            homepage_official_source=source,
            homepage_official_score=float(homepage_official_score or 0.0),
            chosen_domain_score=int(chosen_domain_score or 0),
            dropped=False,
        )

    domain_score = int(chosen_domain_score or 0)
    strong_provisional = (
        (domain_score >= 4)
        or (bool(provisional_host_token) and domain_score >= 3)
        or (bool(provisional_name_present) and domain_score >= 3)
        or (bool(provisional_address_ok) and domain_score >= 4)
    )
    if provisional_ai_hint or provisional_profile_hit or int(provisional_evidence_score or 0) >= 8:
        strong_provisional = True
    if strong_provisional:
        return HomepageDecision(
            homepage=homepage,
            homepage_official_flag=0,
            homepage_official_source=source,
            homepage_official_score=float(homepage_official_score or 0.0),
            chosen_domain_score=domain_score,
            dropped=False,
        )

    return HomepageDecision(
        homepage="",
        homepage_official_flag=0,
        homepage_official_source="",
        homepage_official_score=0.0,
        chosen_domain_score=0,
        dropped=True,
    )

