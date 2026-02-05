#!/usr/bin/env python3
import argparse
import os
import subprocess
import sys


def run(cmd: list[str], label: str, env: dict | None = None) -> None:
    print(f"[{label}] running: {' '.join(cmd)}")
    result = subprocess.run(cmd, env=env)
    if result.returncode != 0:
        raise SystemExit(result.returncode)


def main() -> int:
    ap = argparse.ArgumentParser(description="Run main scraper then contact URL extraction in one command.")
    ap.add_argument("--db", default=os.getenv("COMPANIES_DB_PATH", "data/companies.db"), help="SQLite DB path")
    ap.add_argument("--skip-contact", action="store_true", help="Skip contact URL extraction step")
    ap.add_argument("--contact-limit", type=int, default=0, help="Max rows for contact URL extraction (0 = no limit)")
    ap.add_argument("--contact-force", action="store_true", help="Overwrite existing contact_url")
    ap.add_argument("--contact-ai", action="store_true", help="Enable AI-based contact form gate")
    ap.add_argument("--contact-ai-min-confidence", type=float, default=None, help="AI confidence threshold")
    ap.add_argument("--contact-progress", action="store_true", help="Print progress per company")
    ap.add_argument("--contact-progress-detail", action="store_true", help="Print detailed result per company")
    ap.add_argument("--contact-status-in", default="", help="Comma-separated contact_url_status values to reprocess")
    ap.add_argument("--contact-reprocess-recommended", action="store_true", help="Reprocess timeout/ai_unsure/ai_not_official with error reasons")
    args = ap.parse_args()

    py = sys.executable or "python"
    env = os.environ.copy()
    env["COMPANIES_DB_PATH"] = args.db
    run([py, "-u", "main.py"], "scrape", env=env)

    if args.skip_contact:
        return 0

    contact_cmd = [py, "-u", "scripts/extract_contact_urls.py", "--db", args.db]
    if args.contact_limit and args.contact_limit > 0:
        contact_cmd += ["--limit", str(args.contact_limit)]
    if args.contact_force:
        contact_cmd.append("--force")
    if args.contact_ai:
        contact_cmd.append("--ai")
    if args.contact_ai_min_confidence is not None:
        contact_cmd += ["--ai-min-confidence", str(args.contact_ai_min_confidence)]
    if args.contact_progress:
        contact_cmd.append("--progress")
    if args.contact_progress_detail:
        contact_cmd.append("--progress-detail")
    if args.contact_status_in:
        contact_cmd += ["--status-in", args.contact_status_in]
    if args.contact_reprocess_recommended:
        contact_cmd.append("--reprocess-recommended")

    run(contact_cmd, "contact", env=env)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
