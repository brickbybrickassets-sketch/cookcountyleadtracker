#!/usr/bin/env python3
"""
Cook County Motivated Seller Lead Scraper - FINAL VERSION
Uses Playwright to handle JS-rendered portal at crs.cookcountyclerkil.gov
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

# ── Configuration ─────────────────────────────────────────────────────────────
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))
DASHBOARD_DIR = Path(os.getenv("DASHBOARD_DIR", "dashboard"))
DATA_DIR      = Path(os.getenv("DATA_DIR", "data"))
GHL_CSV_PATH  = Path(os.getenv("GHL_CSV_PATH", "data/ghl_export.csv"))

CLERK_BASE  = "https://crs.cookcountyclerkil.gov"
SEARCH_URL  = f"{CLERK_BASE}/Search/Additional"

# Exact document type names from the portal dropdown
DOC_TYPES = [
    ("LIS PENDENS",                   "LP",      "Lis Pendens"),
    ("AMENDED LIS PENDENS",           "LP",      "Amended Lis Pendens"),
    ("JUDGMENT",                      "JUD",     "Judgment"),
    ("LIEN",                          "LIEN",    "Lien"),
    ("FEDERAL LIEN",                  "LIEN",    "Federal Lien"),
    ("AMENDED FEDERAL TAX LIEN",      "LIEN",    "Amended Federal Tax Lien"),
    ("CHILD SUPPORT LIEN",            "LIEN",    "Child Support Lien"),
    ("BANKRUPTCY",                    "PRO",     "Bankruptcy"),
    ("FORECLOSURE TITLE FREEZE",      "NOFC",    "Foreclosure Title Freeze"),
    ("CERTIFICATE OF PURCHASE",       "TAXDEED", "Certificate of Purchase"),
    ("CERTIFICATE OF LEVY",           "LIEN",    "Certificate of Levy"),
    ("NOTICE PROP TAX DEFERRAL LIEN", "LIEN",    "Property Tax Deferral Lien"),
    ("AMENDED LIS PENDENS FORECLOSURE","NOFC",   "Amended LP Foreclosure"),
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("cook_scraper")


# ── Scoring ───────────────────────────────────────────────────────────────────

def filed_this_week(filed_str):
    if not filed_str:
        return False
    try:
        return (datetime.utcnow() - datetime.strptime(filed_str[:10], "%Y-%m-%d")).days <= 7
    except Exception:
        return False


def compute_flags(r):
    flags = []
    cat   = r.get("cat", "")
    owner = (r.get("owner") or "").upper()
    doc   = (r.get("doc_type") or "").upper()
    if cat == "LP":                                            flags.append("Lis pendens")
    if cat in ("NOFC", "LP"):                                 flags.append("Pre-foreclosure")
    if cat == "JUD":                                          flags.append("Judgment lien")
    if "LIEN" in doc or "LEVY" in doc:                        flags.append("Tax lien")
    if cat == "PRO" or "BANKRUPT" in doc:                     flags.append("Probate / estate")
    if re.search(r"\b(LLC|INC|CORP|LTD|TRUST)\b", owner):    flags.append("LLC / corp owner")
    if filed_this_week(r.get("filed")):                       flags.append("New this week")
    return flags


def compute_score(r, flags):
    score  = 0
    cat    = r.get("cat", "")
    amount = r.get("amount") or 0
    if "Tax lien" in flags:                                   score += 30
    if cat == "PRO":                                          score += 25
    if cat in ("LP", "NOFC"):                                 score += 20
    if cat == "JUD":                                          score += 10
    if len([f for f in flags if "lien" in f.lower()]) > 1:   score += 15
    if amount > 100_000:                                      score += 15
    elif amount > 50_000:                                     score += 10
    if filed_this_week(r.get("filed")):                       score += 5
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


# ── Playwright Scraper ────────────────────────────────────────────────────────

async def scrape_all(date_from, date_to):
    from playwright.async_api import async_playwright, TimeoutError as PWTimeout

    all_records = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800},
        )
        page = await context.new_page()

        for doc_type_name, cat, cat_label in DOC_TYPES:
            log.info(f"Fetching: {doc_type_name}")
            try:
                records = await scrape_one_type(page, doc_type_name, cat, cat_label, date_from, date_to)
                all_records.extend(records)
                log.info(f"  -> {len(records)} records")
            except Exception as e:
                log.error(f"Error on {doc_type_name}: {e}")
                log.debug(traceback.format_exc())
            await asyncio.sleep(1)

        await browser.close()

    return all_records


async def scrape_one_type(page, doc_type_name, cat, cat_label, date_from, date_to):
    from playwright.async_api import TimeoutError as PWTimeout
    records = []

    for attempt in range(3):
        try:
            # Navigate to search page
            await page.goto(SEARCH_URL, timeout=30000, wait_until="domcontentloaded")
            await page.wait_for_timeout(1500)

            # Click "Document Type Search" accordion to expand it
            try:
                await page.click("text=Document Type Search", timeout=5000)
                await page.wait_for_timeout(500)
            except PWTimeout:
                pass

            # Select document type from dropdown
            await page.select_option("select[name='DocumentType'], select#DocumentType", 
                                     label=doc_type_name, timeout=5000)

            # Fill date fields
            await page.fill("input[name='FromDate'], input#FromDate", date_from, timeout=5000)
            await page.fill("input[name='ToDate'], input#ToDate", date_to, timeout=5000)

            # Click Search button
            await page.click("button:has-text('Search'), input[value='Search']", timeout=5000)
            await page.wait_for_timeout(3000)

            # Get all pages of results
            page_num = 1
            while True:
                html = await page.content()
                page_records = parse_html_results(html, doc_type_name, cat, cat_label)
                records.extend(page_records)

                # Check for next page
                try:
                    next_btn = page.locator("a:has-text('Next'), li.next a, a[aria-label='Next']").first
                    if await next_btn.is_visible(timeout=2000):
                        await next_btn.click()
                        await page.wait_for_timeout(2000)
                        page_num += 1
                        log.info(f"  Page {page_num} for {doc_type_name}")
                    else:
                        break
                except Exception:
                    break

            return records

        except Exception as e:
            log.warning(f"Attempt {attempt+1} failed for {doc_type_name}: {e}")
            if attempt == 2:
                return records
            await asyncio.sleep(2)

    return records


def parse_html_results(html, doc_type_name, cat, cat_label):
    from bs4 import BeautifulSoup
    records = []
    soup = BeautifulSoup(html, "lxml")

    # Skip if no results message
    if soup.find(string=re.compile("No Document", re.I)):
        return records

    # Try all common table selectors
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
            if any(h in combined for h in ("document", "recorded", "grantor", "type", "number", "date")):
                continue

            doc_num = cells[0].strip()
            if not doc_num or len(doc_num) < 3:
                continue

            filed   = parse_date(cells[1] if len(cells) > 1 else "")
            grantor = cells[2].strip() if len(cells) > 2 else ""
            grantee = cells[3].strip() if len(cells) > 3 else ""
            amount  = parse_amount(cells[4] if len(cells) > 4 else "")
            legal   = cells[5].strip() if len(cells) > 5 else ""

            link     = row.find("a", href=True)
            clerk_url = (f"{CLERK_BASE}{link['href']}" if link and not link['href'].startswith('http') 
                        else (link['href'] if link else f"{CLERK_BASE}/Document/Detail?dId={doc_num}"))

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
                "grantee":          grantee,
                "amount":           amount,
                "legal":            legal,
                "clerk_url":        clerk_url,
                "prop_address":     "", "prop_city": "", "prop_state": "IL", "prop_zip": "",
                "mail_address":     "", "mail_city": "", "mail_state": "IL", "mail_zip": "",
                "needs_enrichment": len(missing) > 0,
                "missing_fields":   missing,
                "scraped_at":       datetime.utcnow().isoformat() + "Z",
            })
        except Exception as e:
            log.warning(f"Skipping bad row: {e}")

    return records


# ── Output ────────────────────────────────────────────────────────────────────

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
                "First Name":             parts[0] if parts else "",
                "Last Name":              " ".join(parts[1:]) if len(parts) > 1 else "",
                "Mailing Address":        r.get("mail_address",""),
                "Mailing City":           r.get("mail_city",""),
                "Mailing State":          r.get("mail_state",""),
                "Mailing Zip":            r.get("mail_zip",""),
                "Property Address":       r.get("prop_address",""),
                "Property City":          r.get("prop_city",""),
                "Property State":         r.get("prop_state",""),
                "Property Zip":           r.get("prop_zip",""),
                "Lead Type":              r.get("cat_label",""),
                "Document Type":          r.get("doc_type",""),
                "Date Filed":             r.get("filed",""),
                "Document Number":        r.get("doc_num",""),
                "Amount / Debt Owed":     r.get("amount",""),
                "Seller Score":           score,
                "Score Tier":             score_tier(score),
                "Motivated Seller Flags": "; ".join(r.get("flags",[])),
                "Needs Enrichment":       "YES" if r.get("needs_enrichment") else "NO",
                "Missing Fields":         ", ".join(r.get("missing_fields",[])),
                "Source":                 "Cook County Clerk Recordings",
                "Public Records URL":     r.get("clerk_url",""),
                "Scraped At":             r.get("scraped_at",""),
            })
    log.info(f"CSV saved -> {GHL_CSV_PATH}")


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    log.info("=" * 60)
    log.info("Cook County Lead Scraper — FINAL VERSION")
    log.info(f"Portal: {SEARCH_URL}")
    log.info("=" * 60)

    date_to   = datetime.utcnow().strftime("%m/%d/%Y")
    date_from = (datetime.utcnow() - timedelta(days=LOOKBACK_DAYS)).strftime("%m/%d/%Y")
    log.info(f"Date range: {date_from} to {date_to}")

    all_records = await scrape_all(date_from, date_to)

    # Deduplicate
    seen   = set()
    unique = []
    for r in all_records:
        key = (r["doc_num"], r["doc_type"])
        if key not in seen and r["doc_num"]:
            seen.add(key)
            flags      = compute_flags(r)
            r["flags"] = flags
            r["score"] = compute_score(r, flags)
            r["tier"]  = score_tier(r["score"])
            unique.append(r)

    unique.sort(key=lambda r: r.get("score", 0), reverse=True)
    save_outputs(unique, date_from, date_to)

    log.info("=" * 60)
    log.info(f"COMPLETE: {len(unique)} total leads")
    log.info(f"  HOT  (70+):   {sum(1 for r in unique if r.get('score',0) >= 70)}")
    log.info(f"  WARM (40-69): {sum(1 for r in unique if 40 <= r.get('score',0) < 70)}")
    log.info(f"  COLD (<40):   {sum(1 for r in unique if r.get('score',0) < 40)}")
    log.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
