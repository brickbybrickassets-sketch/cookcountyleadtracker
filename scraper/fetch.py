#!/usr/bin/env python3
"""
Cook County Motivated Seller Lead Scraper
Portal: crs.cookcountyclerkil.gov/Search/Additional
"""

import csv
import json
import logging
import os
import re
import time
import traceback
import urllib3
from datetime import datetime, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    from rapidfuzz import fuzz
    FUZZY_AVAILABLE = True
except ImportError:
    FUZZY_AVAILABLE = False

LOOKBACK_DAYS   = int(os.getenv("LOOKBACK_DAYS", "7"))
DASHBOARD_DIR   = Path(os.getenv("DASHBOARD_DIR", "dashboard"))
DATA_DIR        = Path(os.getenv("DATA_DIR", "data"))
GHL_CSV_PATH    = Path(os.getenv("GHL_CSV_PATH", "data/ghl_export.csv"))
FUZZY_THRESHOLD = 80

CLERK_BASE  = "https://crs.cookcountyclerkil.gov"
SEARCH_URL  = f"{CLERK_BASE}/Search/Additional"
RESULT_URL  = f"{CLERK_BASE}/Search/ResultAddt?id1=%23collapse3"

# Exact document type names from the portal dropdown
DOC_TYPE_MAP = {
    "LIS PENDENS":                  ("LP",      "Lis Pendens"),
    "AMENDED LIS PENDENS":          ("LP",      "Amended Lis Pendens"),
    "AMENDED LIS PENDENS FORECLOSURE": ("NOFC", "Amended LP Foreclosure"),
    "FEDERAL LIEN":                 ("LIEN",    "Federal Lien"),
    "AMENDED FEDERAL TAX LIEN":     ("LIEN",    "Amended Federal Tax Lien"),
    "JUDGMENT":                     ("JUD",     "Judgment"),
    "LIEN":                         ("LIEN",    "Lien"),
    "CHILD SUPPORT LIEN":           ("LIEN",    "Child Support Lien"),
    "NOTICE PROP TAX DEFERRAL LIEN":("LIEN",    "Property Tax Deferral Lien"),
    "BANKRUPTCY":                   ("PRO",     "Bankruptcy"),
    "FORECLOSURE TITLE FREEZE":     ("NOFC",    "Foreclosure Title Freeze"),
    "CERTIFICATE OF LEVY":          ("LIEN",    "Certificate of Levy"),
    "CERTIFICATE OF PURCHASE":      ("TAXDEED", "Certificate of Purchase"),
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("cook_scraper")

HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         SEARCH_URL,
}
SESSION = requests.Session()
SESSION.headers.update(HEADERS)
SESSION.verify = False


def normalize_name(name):
    if not name:
        return ""
    name = re.sub(r"[^\w\s]", " ", name.strip().lower())
    name = re.sub(r"\s+", " ", name).strip()
    if "," in name:
        parts = name.split(",", 1)
        name = f"{parts[1].strip()} {parts[0].strip()}"
    return name


def filed_this_week(filed_str):
    if not filed_str:
        return False
    try:
        return (datetime.utcnow() - datetime.strptime(filed_str[:10], "%Y-%m-%d")).days <= 7
    except Exception:
        return False


def compute_flags(r):
    flags    = []
    cat      = r.get("cat", "")
    doc_type = r.get("doc_type", "")
    owner    = (r.get("owner") or "").upper()
    if cat == "LP":                                             flags.append("Lis pendens")
    if cat in ("NOFC", "LP"):                                  flags.append("Pre-foreclosure")
    if cat == "JUD":                                           flags.append("Judgment lien")
    if "LIEN" in doc_type.upper() or "LEVY" in doc_type.upper(): flags.append("Tax lien")
    if cat == "PRO" or "BANKRUPTCY" in doc_type.upper():       flags.append("Probate / estate")
    if re.search(r"\b(LLC|INC|CORP|LTD|TRUST)\b", owner):     flags.append("LLC / corp owner")
    if filed_this_week(r.get("filed")):                        flags.append("New this week")
    if r.get("needs_enrichment"):                              flags.append("Needs enrichment")
    return flags


def compute_score(r, flags):
    score    = 0
    cat      = r.get("cat", "")
    amount   = r.get("amount") or 0
    match_sc = r.get("match_score") or 0
    if "Tax lien" in flags:                                    score += 30
    if cat == "PRO":                                           score += 25
    if cat in ("LP", "NOFC"):                                  score += 20
    if cat == "JUD":                                           score += 10
    if len([f for f in flags if "lien" in f.lower()]) > 1:    score += 15
    if match_sc >= FUZZY_THRESHOLD:                            score += 10
    if amount > 100_000:                                       score += 15
    elif amount > 50_000:                                      score += 10
    if filed_this_week(r.get("filed")):                        score += 5
    if r.get("needs_enrichment"):                              score -= 10
    return max(0, min(score, 100))


def score_tier(score):
    if score >= 70:   return "hot"
    elif score >= 40: return "warm"
    return "cold"


def parse_amount(text):
    try:
        return float(re.sub(r"[^\d.]", "", str(text)))
    except Exception:
        return 0.0


def parse_date(text):
    if not text:
        return ""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            return datetime.strptime(text.strip(), fmt).strftime("%Y-%m-%d")
        except Exception:
            pass
    return text.strip()


def scrape_doc_type(doc_type_name, cat, cat_label, date_from, date_to):
    records = []
    log.info(f"Fetching {doc_type_name}")

    for attempt in range(3):
        try:
            # Load search page to get cookies
            resp = SESSION.get(SEARCH_URL, timeout=20)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "lxml")

            # Get hidden fields
            hidden = {}
            for inp in soup.find_all("input", {"type": "hidden"}):
                if inp.get("name"):
                    hidden[inp["name"]] = inp.get("value", "")

            # Submit form — match exact field names from portal
            form_data = {
                **hidden,
                "DocumentType": doc_type_name,
                "FromDate":     date_from,
                "ToDate":       date_to,
                "LowerLimit":   "",
                "UpperLimit":   "",
            }

            resp2 = SESSION.post(RESULT_URL, data=form_data, timeout=20)
            resp2.raise_for_status()
            soup2 = BeautifulSoup(resp2.text, "lxml")

            page_records = parse_results(soup2, doc_type_name, cat, cat_label)
            records.extend(page_records)
            log.info(f"  -> {len(records)} records for {doc_type_name}")
            return records

        except Exception as e:
            log.warning(f"Attempt {attempt+1} failed for {doc_type_name}: {e}")
            if attempt == 2:
                log.error(f"All attempts failed for {doc_type_name}")
                return records
            time.sleep(2)

    return records


def parse_results(soup, doc_type_name, cat, cat_label):
    records = []

    # Try multiple table selectors
    rows = (
        soup.select("table.table tbody tr") or
        soup.select("table tbody tr") or
        soup.select("table tr")
    )

    for row in rows:
        try:
            cells = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
            if len(cells) < 2:
                continue
            combined = " ".join(cells[:3]).lower()
            if any(h in combined for h in ("document", "recorded", "grantor", "type", "date")):
                continue

            doc_num = cells[0].strip()
            if not doc_num or len(doc_num) < 3:
                continue

            filed   = parse_date(cells[1] if len(cells) > 1 else "")
            grantor = cells[2].strip() if len(cells) > 2 else ""
            grantee = cells[3].strip() if len(cells) > 3 else ""
            amount  = parse_amount(cells[4] if len(cells) > 4 else "")
            legal   = cells[5].strip() if len(cells) > 5 else ""

            # Get direct link if available
            link    = row.find("a", href=True)
            clerk_url = f"{CLERK_BASE}{link['href']}" if link else f"{CLERK_BASE}/Document/Details/{doc_num}"

            missing = []
            if not grantor: missing.append("grantor")
            if not legal:   missing.append("legal_description")
            if not filed:   missing.append("filed_date")

            records.append({
                "doc_num":          doc_num,
                "doc_type":         doc_type_name,
                "filed":            filed,
                "cat":              cat,
                "cat_label":        cat_label,
                "owner":            grantor,
                "owner_normalized": normalize_name(grantor),
                "grantee":          grantee,
                "amount":           amount,
                "legal":            legal,
                "clerk_url":        clerk_url,
                "prop_address":     "", "prop_city": "", "prop_state": "IL", "prop_zip": "",
                "mail_address":     "", "mail_city": "", "mail_state": "IL", "mail_zip": "",
                "needs_enrichment": len(missing) > 0,
                "missing_fields":   missing,
                "match_score":      0,
                "scraped_at":       datetime.utcnow().isoformat() + "Z",
            })
        except Exception as e:
            log.warning(f"Skipping bad row: {e}")

    return records


def save_outputs(records, date_from, date_to):
    DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "fetched_at":       datetime.utcnow().isoformat() + "Z",
        "source":           "Cook County Clerk Recordings",
        "date_range":       {"from": date_from, "to": date_to},
        "total":            len(records),
        "with_address":     sum(1 for r in records if r.get("prop_address")),
        "needs_enrichment": sum(1 for r in records if r.get("needs_enrichment")),
        "score_breakdown":  {
            "hot":  sum(1 for r in records if r.get("score", 0) >= 70),
            "warm": sum(1 for r in records if 40 <= r.get("score", 0) < 70),
            "cold": sum(1 for r in records if r.get("score", 0) < 40),
        },
        "records": records,
    }

    for path in [DASHBOARD_DIR / "records.json", DATA_DIR / "records.json"]:
        path.write_text(json.dumps(payload, indent=2))
    log.info(f"JSON saved — {len(records)} records")

    GHL_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "First Name","Last Name",
        "Mailing Address","Mailing City","Mailing State","Mailing Zip",
        "Property Address","Property City","Property State","Property Zip",
        "Lead Type","Document Type","Date Filed","Document Number",
        "Amount / Debt Owed","Seller Score","Score Tier",
        "Motivated Seller Flags","Needs Enrichment","Missing Fields",
        "Source","Public Records URL","Scraped At",
    ]
    with open(GHL_CSV_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in records:
            parts = (r.get("owner") or "").split()
            score = r.get("score", 0)
            w.writerow({
                "First Name":            parts[0] if parts else "",
                "Last Name":             " ".join(parts[1:]) if len(parts) > 1 else "",
                "Mailing Address":       r.get("mail_address",""),
                "Mailing City":          r.get("mail_city",""),
                "Mailing State":         r.get("mail_state",""),
                "Mailing Zip":           r.get("mail_zip",""),
                "Property Address":      r.get("prop_address",""),
                "Property City":         r.get("prop_city",""),
                "Property State":        r.get("prop_state",""),
                "Property Zip":          r.get("prop_zip",""),
                "Lead Type":             r.get("cat_label",""),
                "Document Type":         r.get("doc_type",""),
                "Date Filed":            r.get("filed",""),
                "Document Number":       r.get("doc_num",""),
                "Amount / Debt Owed":    r.get("amount",""),
                "Seller Score":          score,
                "Score Tier":            score_tier(score),
                "Motivated Seller Flags": "; ".join(r.get("flags",[])),
                "Needs Enrichment":      "YES" if r.get("needs_enrichment") else "NO",
                "Missing Fields":        ", ".join(r.get("missing_fields",[])),
                "Source":                "Cook County Clerk Recordings",
                "Public Records URL":    r.get("clerk_url",""),
                "Scraped At":            r.get("scraped_at",""),
            })
    log.info(f"CSV saved -> {GHL_CSV_PATH}")


def main():
    log.info("=" * 60)
    log.info("Cook County Lead Scraper — Real Portal")
    log.info(f"Portal: {SEARCH_URL}")
    log.info("=" * 60)

    date_to   = datetime.utcnow().strftime("%m/%d/%Y")
    date_from = (datetime.utcnow() - timedelta(days=LOOKBACK_DAYS)).strftime("%m/%d/%Y")
    log.info(f"Date range: {date_from} to {date_to}")

    all_records = []
    seen        = set()

    for doc_type_name, (cat, cat_label) in DOC_TYPE_MAP.items():
        try:
            for r in scrape_doc_type(doc_type_name, cat, cat_label, date_from, date_to):
                key = (r["doc_num"], r["doc_type"])
                if key not in seen and r["doc_num"]:
                    seen.add(key)
                    flags      = compute_flags(r)
                    r["flags"] = flags
                    r["score"] = compute_score(r, flags)
                    r["tier"]  = score_tier(r["score"])
                    all_records.append(r)
        except Exception as e:
            log.error(f"Error on {doc_type_name}: {e}")
            log.debug(traceback.format_exc())

    all_records.sort(key=lambda r: r.get("score", 0), reverse=True)
    save_outputs(all_records, date_from, date_to)

    log.info("=" * 60)
    log.info(f"COMPLETE: {len(all_records)} total leads")
    log.info(f"  HOT  (70+):   {sum(1 for r in all_records if r.get('score',0) >= 70)}")
    log.info(f"  WARM (40-69): {sum(1 for r in all_records if 40 <= r.get('score',0) < 70)}")
    log.info(f"  COLD (<40):   {sum(1 for r in all_records if r.get('score',0) < 40)}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
