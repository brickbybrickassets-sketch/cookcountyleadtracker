#!/usr/bin/env python3
"""Cook County Motivated Seller Lead Scraper - Minimal Version"""
 
import csv
import json
import logging
import os
import re
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path
 
import urllib3
import requests
from bs4 import BeautifulSoup
 
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
 
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))
DASHBOARD_DIR = Path(os.getenv("DASHBOARD_DIR", "dashboard"))
DATA_DIR      = Path(os.getenv("DATA_DIR", "data"))
GHL_CSV_PATH  = Path(os.getenv("GHL_CSV_PATH", "data/ghl_export.csv"))
 
CLERK_BASE   = "https://ccrecorder.org"
CLERK_SEARCH = f"{CLERK_BASE}/Search/SearchEntry"
 
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
 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("cook_scraper")
 
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}
SESSION = requests.Session()
SESSION.headers.update(HEADERS)
SESSION.verify = False
 
 
def filed_this_week(filed_str):
    if not filed_str:
        return False
    try:
        filed = datetime.strptime(filed_str[:10], "%Y-%m-%d")
        return (datetime.utcnow() - filed).days <= 7
    except Exception:
        return False
 
 
def compute_flags(r):
    flags = []
    cat      = r.get("cat", "")
    doc_type = r.get("doc_type", "")
    owner    = (r.get("owner") or "").upper()
    if cat == "LP":
        flags.append("Lis pendens")
    if cat in ("NOFC", "LP"):
        flags.append("Pre-foreclosure")
    if cat == "JUD":
        flags.append("Judgment lien")
    if doc_type in ("LNCORPTX", "LNIRS", "LNFED", "TAXDEED"):
        flags.append("Tax lien")
    if doc_type == "LNMECH":
        flags.append("Mechanic lien")
    if cat == "PRO":
        flags.append("Probate / estate")
    if re.search(r"\b(LLC|INC|CORP|LTD|TRUST)\b", owner):
        flags.append("LLC / corp owner")
    if filed_this_week(r.get("filed")):
        flags.append("New this week")
    return flags
 
 
def compute_score(r, flags):
    score  = 30 + 10 * len(flags)
    amount = r.get("amount") or 0
    if r.get("cat") == "LP":
        score += 20
    if amount > 100000:
        score += 15
    elif amount > 50000:
        score += 10
    if filed_this_week(r.get("filed")):
        score += 5
    return min(score, 100)
 
 
def parse_amount(text):
    if not text:
        return 0.0
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
 
 
def scrape_doc_type(doc_type, date_from, date_to):
    cat, cat_label = DOC_TYPE_MAP.get(doc_type, (doc_type, doc_type))
    url = (
        f"{CLERK_SEARCH}?DocType={doc_type}"
        f"&DateFrom={date_from}&DateTo={date_to}"
        f"&County=Cook&State=IL"
    )
    log.info(f"Fetching {doc_type} -> {url}")
    records = []
 
    for attempt in range(3):
        try:
            resp = SESSION.get(url, timeout=20)
            resp.raise_for_status()
            break
        except Exception as e:
            log.warning(f"Attempt {attempt+1} failed: {e}")
            if attempt == 2:
                return records
            time.sleep(3)
 
    soup = BeautifulSoup(resp.text, "lxml")
 
    # Try common table selectors
    rows = (
        soup.select("table#GridView1 tr") or
        soup.select("table.results tr") or
        soup.select("table tr")
    )
 
    for row in rows:
        cells = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
        if len(cells) < 3:
            continue
        # Skip header rows
        combined = " ".join(cells[:4]).lower()
        if any(h in combined for h in ("document", "filed date", "grantor", "instrument")):
            continue
 
        doc_num = cells[0].strip()
        if not doc_num or len(doc_num) < 3:
            continue
 
        records.append({
            "doc_num":   doc_num,
            "doc_type":  doc_type,
            "filed":     parse_date(cells[1] if len(cells) > 1 else ""),
            "cat":       cat,
            "cat_label": cat_label,
            "owner":     cells[2].strip() if len(cells) > 2 else "",
            "grantee":   cells[3].strip() if len(cells) > 3 else "",
            "amount":    parse_amount(cells[4] if len(cells) > 4 else ""),
            "legal":     cells[5].strip() if len(cells) > 5 else "",
            "clerk_url": f"{CLERK_BASE}/Search/DocDisplay?DocNum={doc_num}",
            "prop_address": "", "prop_city": "", "prop_state": "IL", "prop_zip": "",
            "mail_address": "", "mail_city": "", "mail_state": "IL", "mail_zip": "",
        })
 
    log.info(f"  -> {len(records)} records for {doc_type}")
    return records
 
 
def save_outputs(records, date_from, date_to):
    DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
 
    payload = {
        "fetched_at":   datetime.utcnow().isoformat() + "Z",
        "source":       "Cook County Recorder of Deeds",
        "date_range":   {"from": date_from, "to": date_to},
        "total":        len(records),
        "with_address": 0,
        "records":      records,
    }
 
    for path in [DASHBOARD_DIR / "records.json", DATA_DIR / "records.json"]:
        path.write_text(json.dumps(payload, indent=2))
    log.info(f"Saved {len(records)} records to JSON")
 
    GHL_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "First Name","Last Name","Mailing Address","Mailing City","Mailing State","Mailing Zip",
        "Property Address","Property City","Property State","Property Zip",
        "Lead Type","Document Type","Date Filed","Document Number",
        "Amount / Debt Owed","Seller Score","Motivated Seller Flags","Source","Public Records URL",
    ]
    with open(GHL_CSV_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in records:
            parts = (r.get("owner") or "").split()
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
                "Seller Score":          r.get("score",""),
                "Motivated Seller Flags": "; ".join(r.get("flags",[])),
                "Source":                "Cook County Recorder of Deeds",
                "Public Records URL":    r.get("clerk_url",""),
            })
    log.info(f"Saved GHL CSV -> {GHL_CSV_PATH}")
 
 
def main():
    log.info("=" * 50)
    log.info("Cook County Lead Scraper Starting")
    log.info("=" * 50)
 
    date_to   = datetime.utcnow().strftime("%m/%d/%Y")
    date_from = (datetime.utcnow() - timedelta(days=LOOKBACK_DAYS)).strftime("%m/%d/%Y")
    log.info(f"Date range: {date_from} to {date_to}")
 
    all_records = []
    seen = set()
 
    for doc_type in DOC_TYPE_MAP:
        try:
            records = scrape_doc_type(doc_type, date_from, date_to)
            for r in records:
                key = (r["doc_num"], r["doc_type"])
                if key not in seen and r["doc_num"]:
                    seen.add(key)
                    flags       = compute_flags(r)
                    r["flags"]  = flags
                    r["score"]  = compute_score(r, flags)
                    all_records.append(r)
        except Exception as e:
            log.error(f"Error on {doc_type}: {e}")
            log.debug(traceback.format_exc())
 
    all_records.sort(key=lambda r: r.get("score", 0), reverse=True)
    save_outputs(all_records, date_from, date_to)
 
    log.info("=" * 50)
    log.info(f"DONE: {len(all_records)} total leads")
    log.info(f"  Hot (>=70): {sum(1 for r in all_records if r.get('score',0) >= 70)}")
    log.info("=" * 50)
 
 
if __name__ == "__main__":
    main()
