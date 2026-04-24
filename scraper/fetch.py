#!/usr/bin/env python3
"""
Cook County Motivated Seller Lead Scraper
Scrapes clerk portal for distressed property records.
"""

import asyncio
import csv
import json
import logging
import os
import re
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

# ── Configuration ─────────────────────────────────────────────────────────────
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))
DASHBOARD_DIR = Path(os.getenv("DASHBOARD_DIR", "dashboard"))
DATA_DIR      = Path(os.getenv("DATA_DIR", "data"))
GHL_CSV_PATH  = Path(os.getenv("GHL_CSV_PATH", "data/ghl_export.csv"))

# Cook County Recorder of Deeds
CLERK_BASE   = "https://ccrecorder.org"
CLERK_SEARCH = f"{CLERK_BASE}/Search/SearchEntry"

# Document type → category mapping
DOC_TYPE_MAP = {
    "LP":       ("LP",      "Lis Pendens"),
    "RELLP":    ("RELLP",   "Release Lis Pendens"),
    "NOFC":     ("NOFC",    "Notice of Foreclosure"),
    "TAXDEED":  ("TAXDEED", "Tax Deed"),
    "JUD":      ("JUD",     "Judgment"),
    "CCJ":      ("JUD",     "Certified Judgment"),
    "DRJUD":    ("JUD",     "Domestic Judgment"),
    "LNCORPTX": ("LIEN",    "Corp Tax Lien"),
    "LNIRS":    ("LIEN",    "IRS Lien"),
    "LNFED":    ("LIEN",    "Federal Lien"),
    "LN":       ("LIEN",    "Lien"),
    "LNMECH":   ("LIEN",    "Mechanic Lien"),
    "LNHOA":    ("LIEN",    "HOA Lien"),
    "MEDLN":    ("LIEN",    "Medicaid Lien"),
    "PRO":      ("PRO",     "Probate"),
    "NOC":      ("NOC",     "Notice of Commencement"),
}

TARGET_TYPES = list(DOC_TYPE_MAP.keys())

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("cook_scraper")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 – SCORING ENGINE
# ══════════════════════════════════════════════════════════════════════════════

FLAG_RULES = {
    "Lis pendens":      lambda r: r.get("cat") == "LP",
    "Pre-foreclosure":  lambda r: r.get("cat") in ("NOFC", "LP"),
    "Judgment lien":    lambda r: r.get("cat") == "JUD",
    "Tax lien":         lambda r: r.get("doc_type") in ("LNCORPTX", "LNIRS", "LNFED", "TAXDEED"),
    "Mechanic lien":    lambda r: r.get("doc_type") == "LNMECH",
    "Probate / estate": lambda r: r.get("cat") == "PRO",
    "LLC / corp owner": lambda r: bool(re.search(r"\b(LLC|INC|CORP|LTD|TRUST)\b", (r.get("owner") or ""), re.I)),
    "New this week":    lambda r: _filed_this_week(r.get("filed")),
}


def _filed_this_week(filed_str: Optional[str]) -> bool:
    if not filed_str:
        return False
    try:
        filed = datetime.strptime(filed_str[:10], "%Y-%m-%d")
        return (datetime.utcnow() - filed).days <= 7
    except ValueError:
        return False


def compute_flags(record: dict) -> list:
    return [name for name, fn in FLAG_RULES.items() if fn(record)]


def compute_score(record: dict, flags: list) -> int:
    score  = 30
    score += 10 * len(flags)
    cat    = record.get("cat", "")
    amount = record.get("amount") or 0

    if cat == "LP":
        score += 20
    if amount > 100_000:
        score += 15
    elif amount > 50_000:
        score += 10
    if _filed_this_week(record.get("filed")):
        score += 5

    return min(score, 100)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 – CLERK PORTAL SCRAPING
# ══════════════════════════════════════════════════════════════════════════════

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


def _retry(fn, attempts=3, delay=2.0):
    for i in range(attempts):
        try:
            return fn()
        except Exception as exc:
            if i == attempts - 1:
                raise
            log.warning(f"Attempt {i+1} failed: {exc} – retrying in {delay}s")
            time.sleep(delay)


def _parse_amount(text: str) -> float:
    if not text:
        return 0.0
    cleaned = re.sub(r"[^\d.]", "", str(text))
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def _parse_date(text: str) -> str:
    if not text:
        return ""
    text = text.strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return text


def _clean(text: str) -> str:
    if not text:
        return ""
    return " ".join(str(text).split())


def build_search_url(doc_type: str, date_from: str, date_to: str) -> str:
    return (
        f"{CLERK_SEARCH}?DocType={doc_type}"
        f"&DateFrom={date_from}&DateTo={date_to}"
        f"&County=Cook&State=IL"
    )


def scrape_clerk_requests(doc_type: str, date_from: str, date_to: str) -> list:
    """Use requests + BeautifulSoup to scrape the clerk portal."""
    records = []
    url = build_search_url(doc_type, date_from, date_to)
    log.info(f"[REQ] Scraping {doc_type}: {url}")

    try:
        resp = _retry(lambda: SESSION.get(url, timeout=15))
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        viewstate   = _get_hidden(soup, "__VIEWSTATE")
        eventval    = _get_hidden(soup, "__EVENTVALIDATION")
        viewstategr = _get_hidden(soup, "__VIEWSTATEGENERATOR")

        page_num = 1
        while True:
            page_records = _parse_clerk_html(resp.text, doc_type)
            records.extend(page_records)
            log.info(f"  Page {page_num}: {len(page_records)} records")

            next_link = soup.find("a", string=re.compile(r"Next|>", re.I))
            if not next_link:
                break

            href = next_link.get("href", "")
            post_match = re.search(r"__doPostBack\('([^']+)','([^']*)'\)", href)
            if post_match:
                target, arg = post_match.groups()
                post_data = {
                    "__EVENTTARGET":        target,
                    "__EVENTARGUMENT":      arg,
                    "__VIEWSTATE":          viewstate,
                    "__EVENTVALIDATION":    eventval,
                    "__VIEWSTATEGENERATOR": viewstategr,
                }
                resp = _retry(lambda: SESSION.post(url, data=post_data, timeout=15))
                resp.raise_for_status()
                soup = BeautifulSoup(resp.text, "lxml")
                viewstate   = _get_hidden(soup, "__VIEWSTATE")
                eventval    = _get_hidden(soup, "__EVENTVALIDATION")
                viewstategr = _get_hidden(soup, "__VIEWSTATEGENERATOR")
                page_num += 1
            else:
                break

    except Exception as exc:
        log.warning(f"Scrape error for {doc_type}: {exc}")

    return records


def _get_hidden(soup: BeautifulSoup, name: str) -> str:
    tag = soup.find("input", {"name": name})
    return tag["value"] if tag and tag.get("value") else ""


def _parse_clerk_html(html: str, doc_type: str) -> list:
    records = []
    soup = BeautifulSoup(html, "lxml")
    cat, cat_label = DOC_TYPE_MAP.get(doc_type, (doc_type, doc_type))

    rows = (
        soup.select("table.results tr")
        or soup.select("table#GridView1 tr")
        or soup.select("table tr")
    )

    for row in rows:
        cells = row.find_all(["td", "th"])
        if len(cells) < 3:
            continue
        texts = [c.get_text(strip=True) for c in cells]

        if any(h in " ".join(texts[:3]).lower() for h in ("doc", "instrument", "filed", "type")):
            continue

        try:
            doc_num = texts[0] if texts else ""
            filed   = texts[1] if len(texts) > 1 else ""
            grantor = texts[2] if len(texts) > 2 else ""
            grantee = texts[3] if len(texts) > 3 else ""
            amount_str = texts[4] if len(texts) > 4 else ""
            legal   = texts[5] if len(texts) > 5 else ""

            if not doc_num or len(doc_num) < 3:
                continue

            record = {
                "doc_num":   _clean(doc_num),
                "doc_type":  doc_type,
                "filed":     _parse_date(filed),
                "cat":       cat,
                "cat_label": cat_label,
                "owner":     _clean(grantor),
                "grantee":   _clean(grantee),
                "amount":    _parse_amount(amount_str),
                "legal":     _clean(legal),
                "clerk_url": f"{CLERK_BASE}/Search/DocDisplay?DocNum={_clean(doc_num)}",
            }
            records.append(record)
        except Exception:
            continue

    return records


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3 – ENRICHMENT & DEDUP
# ══════════════════════════════════════════════════════════════════════════════

def enrich_record(record: dict) -> dict:
    enriched = {**record}
    enriched.update({
        "prop_address": "",
        "prop_city":    "",
        "prop_state":   "IL",
        "prop_zip":     "",
        "mail_address": "",
        "mail_city":    "",
        "mail_state":   "IL",
        "mail_zip":     "",
    })
    flags             = compute_flags(enriched)
    enriched["flags"] = flags
    enriched["score"] = compute_score(enriched, flags)
    return enriched


def deduplicate(records: list) -> list:
    seen = set()
    out  = []
    for r in records:
        key = (r.get("doc_num", ""), r.get("doc_type", ""))
        if key not in seen and key[0]:
            seen.add(key)
            out.append(r)
    return out


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 4 – OUTPUT WRITERS
# ══════════════════════════════════════════════════════════════════════════════

RECORD_SCHEMA = [
    "doc_num", "doc_type", "filed", "cat", "cat_label",
    "owner", "grantee", "amount", "legal",
    "prop_address", "prop_city", "prop_state", "prop_zip",
    "mail_address", "mail_city", "mail_state", "mail_zip",
    "clerk_url", "flags", "score",
]


def normalise_record(r: dict) -> dict:
    out = {}
    for field in RECORD_SCHEMA:
        v = r.get(field)
        if field == "flags":
            out[field] = v if isinstance(v, list) else []
        elif field in ("amount", "score"):
            out[field] = float(v) if v else 0.0
        else:
            out[field] = str(v).strip() if v else ""
    return out


def save_json(records: list, date_from: str, date_to: str) -> None:
    DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    with_address = sum(1 for r in records if r.get("prop_address"))
    payload = {
        "fetched_at":   datetime.utcnow().isoformat() + "Z",
        "source":       "Cook County Recorder of Deeds",
        "date_range":   {"from": date_from, "to": date_to},
        "total":        len(records),
        "with_address": with_address,
        "records":      [normalise_record(r) for r in records],
    }

    for path in [DASHBOARD_DIR / "records.json", DATA_DIR / "records.json"]:
        path.write_text(json.dumps(payload, indent=2, default=str))
        log.info(f"JSON saved → {path}")


def save_ghl_csv(records: list) -> None:
    GHL_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "First Name", "Last Name",
        "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
        "Property Address", "Property City", "Property State", "Property Zip",
        "Lead Type", "Document Type", "Date Filed", "Document Number",
        "Amount / Debt Owed", "Seller Score", "Motivated Seller Flags",
        "Source", "Public Records URL",
    ]

    with open(GHL_CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in records:
            owner = r.get("owner", "")
            parts = owner.split()
            first = parts[0] if parts else ""
            last  = " ".join(parts[1:]) if len(parts) > 1 else ""
            writer.writerow({
                "First Name":            first,
                "Last Name":             last,
                "Mailing Address":       r.get("mail_address", ""),
                "Mailing City":          r.get("mail_city", ""),
                "Mailing State":         r.get("mail_state", ""),
                "Mailing Zip":           r.get("mail_zip", ""),
                "Property Address":      r.get("prop_address", ""),
                "Property City":         r.get("prop_city", ""),
                "Property State":        r.get("prop_state", ""),
                "Property Zip":          r.get("prop_zip", ""),
                "Lead Type":             r.get("cat_label", ""),
                "Document Type":         r.get("doc_type", ""),
                "Date Filed":            r.get("filed", ""),
                "Document Number":       r.get("doc_num", ""),
                "Amount / Debt Owed":    r.get("amount", ""),
                "Seller Score":          r.get("score", ""),
                "Motivated Seller Flags": "; ".join(r.get("flags", [])),
                "Source":                "Cook County Recorder of Deeds",
                "Public Records URL":    r.get("clerk_url", ""),
            })

    log.info(f"GHL CSV saved → {GHL_CSV_PATH} ({len(records)} rows)")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 5 – MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    log.info("=" * 60)
    log.info("Cook County Motivated Seller Lead Scraper")
    log.info("=" * 60)

    date_to   = datetime.utcnow().strftime("%m/%d/%Y")
    date_from = (datetime.utcnow() - timedelta(days=LOOKBACK_DAYS)).strftime("%m/%d/%Y")
    log.info(f"Date range: {date_from} → {date_to}")

    all_records = []

    for doc_type in TARGET_TYPES:
        log.info(f"\n── Fetching: {doc_type} ──")
        try:
            records = scrape_clerk_requests(doc_type, date_from, date_to)
            log.info(f"  → {len(records)} raw records for {doc_type}")
            all_records.extend(records)
        except Exception as exc:
            log.error(f"Fatal error scraping {doc_type}: {exc}")
            log.debug(traceback.format_exc())

    log.info(f"\nDeduplicating {len(all_records)} raw records …")
    all_records = deduplicate(all_records)
    log.info(f"After dedup: {len(all_records)} unique records")

    log.info("Enriching records and computing scores …")
    enriched = []
    for r in all_records:
        try:
            enriched.append(enrich_record(r))
        except Exception as exc:
            log.warning(f"Enrich failed for {r.get('doc_num')}: {exc}")

    enriched.sort(key=lambda r: r.get("score", 0), reverse=True)

    log.info("\nSaving outputs …")
    save_json(enriched, date_from, date_to)
    save_ghl_csv(enriched)

    log.info("\n" + "=" * 60)
    log.info(f"COMPLETE: {len(enriched)} leads captured")
    log.info(f"  Score >= 70: {sum(1 for r in enriched if r.get('score', 0) >= 70)}")
    log.info(f"  Score >= 50: {sum(1 for r in enriched if r.get('score', 0) >= 50)}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
