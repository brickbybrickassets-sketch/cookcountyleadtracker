#!/usr/bin/env python3
"""
Cook County Motivated Seller Lead Scraper
Scrapes clerk portal for distressed property records and enriches with parcel data.
"""

import asyncio
import csv
import json
import logging
import os
import re
import time
import traceback
import urllib.request
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

# ── Optional imports (graceful fallback) ──────────────────────────────────────
try:
    from playwright.async_api import async_playwright, TimeoutError as PWTimeout
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    logging.warning("Playwright not available – will use requests-only mode")

try:
    from dbfread import DBF
    DBFREAD_AVAILABLE = True
except ImportError:
    DBFREAD_AVAILABLE = False
    logging.warning("dbfread not available – parcel enrichment disabled")

# ── Configuration ─────────────────────────────────────────────────────────────
LOOKBACK_DAYS   = int(os.getenv("LOOKBACK_DAYS", "7"))
OUTPUT_DIR      = Path(os.getenv("OUTPUT_DIR", "."))
DASHBOARD_DIR   = Path(os.getenv("DASHBOARD_DIR", "dashboard"))
DATA_DIR        = Path(os.getenv("DATA_DIR", "data"))
GHL_CSV_PATH    = Path(os.getenv("GHL_CSV_PATH", "data/ghl_export.csv"))
HEADLESS        = os.getenv("HEADLESS", "true").lower() != "false"

# Cook County Recorder of Deeds portal
CLERK_BASE      = "https://ccrecorder.org"
CLERK_SEARCH    = f"{CLERK_BASE}/Search/SearchEntry"

# Cook County Assessor bulk parcel data
PARCEL_BASE_URL = "https://datacatalog.cookcountyil.gov/api/views/tx2p-k2g9/rows.csv?accessType=DOWNLOAD"
PARCEL_CACHE    = Path("/tmp/cook_parcel.csv")

# Document type → category mapping
DOC_TYPE_MAP = {
    # Lis Pendens
    "LP":        ("LP",       "Lis Pendens"),
    "RELLP":     ("RELLP",    "Release Lis Pendens"),
    # Foreclosure
    "NOFC":      ("NOFC",     "Notice of Foreclosure"),
    # Tax / Deed
    "TAXDEED":   ("TAXDEED",  "Tax Deed"),
    # Judgments
    "JUD":       ("JUD",      "Judgment"),
    "CCJ":       ("JUD",      "Certified Judgment"),
    "DRJUD":     ("JUD",      "Domestic Judgment"),
    # Tax / Federal Liens
    "LNCORPTX":  ("LIEN",     "Corp Tax Lien"),
    "LNIRS":     ("LIEN",     "IRS Lien"),
    "LNFED":     ("LIEN",     "Federal Lien"),
    # Property Liens
    "LN":        ("LIEN",     "Lien"),
    "LNMECH":    ("LIEN",     "Mechanic Lien"),
    "LNHOA":     ("LIEN",     "HOA Lien"),
    # Other
    "MEDLN":     ("LIEN",     "Medicaid Lien"),
    "PRO":       ("PRO",      "Probate"),
    "NOC":       ("NOC",      "Notice of Commencement"),
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
# SECTION 1 – PARCEL / ASSESSOR DATA
# ══════════════════════════════════════════════════════════════════════════════

def download_parcel_data() -> dict:
    """
    Download Cook County Assessor parcel CSV (via Socrata open data).
    Returns dict keyed by normalised owner-name variants.
    Falls back to empty dict on any error.
    """
    if not PARCEL_CACHE.exists() or (
        time.time() - PARCEL_CACHE.stat().st_mtime > 86_400
    ):
        log.info("Downloading parcel CSV from Cook County open data …")
        try:
            urllib.request.urlretrieve(PARCEL_BASE_URL, PARCEL_CACHE)
            log.info(f"Parcel CSV saved → {PARCEL_CACHE}")
        except Exception as exc:
            log.warning(f"Parcel download failed: {exc}")
            return {}

    return _parse_parcel_csv(PARCEL_CACHE)


def _parse_parcel_csv(path: Path) -> dict:
    """Parse CSV parcel data into owner-name lookup dict."""
    lookup: dict[str, dict] = {}
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    _index_parcel_row(lookup, row)
                except Exception:
                    pass
        log.info(f"Parcel lookup built – {len(lookup):,} owner entries")
    except Exception as exc:
        log.warning(f"Parcel CSV parse error: {exc}")
    return lookup


def _parse_parcel_dbf(path: Path) -> dict:
    """Parse DBF parcel data into owner-name lookup dict."""
    lookup: dict[str, dict] = {}
    if not DBFREAD_AVAILABLE:
        return lookup
    try:
        table = DBF(str(path), ignore_missing_memofile=True)
        for row in table:
            try:
                _index_parcel_row(lookup, dict(row))
            except Exception:
                pass
        log.info(f"Parcel DBF lookup built – {len(lookup):,} entries")
    except Exception as exc:
        log.warning(f"Parcel DBF parse error: {exc}")
    return lookup


def _col(row: dict, *keys) -> str:
    """Return first non-empty value from a list of possible column names."""
    for k in keys:
        for candidate in (k, k.upper(), k.lower()):
            v = row.get(candidate, "")
            if v and str(v).strip():
                return str(v).strip()
    return ""


def _index_parcel_row(lookup: dict, row: dict) -> None:
    """Index a single parcel row under all owner-name variants."""
    owner = _col(row, "OWNER", "OWN1", "owner", "own1")
    if not owner:
        return

    site_addr  = _col(row, "SITE_ADDR", "SITEADDR", "site_addr", "siteaddr")
    site_city  = _col(row, "SITE_CITY", "sitecity")
    site_zip   = _col(row, "SITE_ZIP", "sitezip")
    mail_addr  = _col(row, "ADDR_1", "MAILADDR", "addr_1", "mailaddr")
    mail_city  = _col(row, "CITY", "MAILCITY", "city", "mailcity")
    mail_state = _col(row, "STATE", "MAILSTATE", "state")
    mail_zip   = _col(row, "ZIP", "MAILZIP", "zip", "mailzip")

    record = {
        "prop_address": site_addr,
        "prop_city":    site_city,
        "prop_state":   "IL",
        "prop_zip":     site_zip,
        "mail_address": mail_addr,
        "mail_city":    mail_city,
        "mail_state":   mail_state or "IL",
        "mail_zip":     mail_zip,
    }

    parts = owner.split()
    for variant in _name_variants(owner, parts):
        key = variant.upper().strip()
        if key:
            lookup.setdefault(key, record)


def _name_variants(raw: str, parts: list[str]) -> list[str]:
    """Generate lookup key variants for an owner name."""
    variants = [raw]
    if len(parts) >= 2:
        first, last = parts[0], parts[-1]
        middle = " ".join(parts[1:-1])
        # "FIRST LAST", "LAST FIRST", "LAST, FIRST"
        variants += [
            f"{first} {last}",
            f"{last} {first}",
            f"{last}, {first}",
        ]
        if middle:
            variants += [
                f"{first} {middle} {last}",
                f"{last} {first} {middle}",
            ]
    return [v.strip() for v in variants if v.strip()]


def lookup_parcel(owner: str, parcel_lookup: dict) -> dict:
    """Find parcel record for owner – tries all name variants."""
    if not owner or not parcel_lookup:
        return {}
    parts = owner.strip().split()
    for variant in _name_variants(owner.strip(), parts):
        hit = parcel_lookup.get(variant.upper())
        if hit:
            return hit
    return {}


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 – SCORING ENGINE
# ══════════════════════════════════════════════════════════════════════════════

FLAG_RULES = {
    "Lis pendens":      lambda r: r.get("cat") in ("LP",),
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


def compute_flags(record: dict) -> list[str]:
    return [name for name, fn in FLAG_RULES.items() if fn(record)]


def compute_score(record: dict, flags: list[str]) -> int:
    score = 30
    score += 10 * len(flags)

    cat      = record.get("cat", "")
    doc_type = record.get("doc_type", "")
    amount   = record.get("amount") or 0

    # LP + FC combo
    if cat in ("LP",) and doc_type in ("NOFC", "LP"):
        score += 20
    # Amount bonuses
    if amount > 100_000:
        score += 15
    elif amount > 50_000:
        score += 10
    # Filed this week
    if _filed_this_week(record.get("filed")):
        score += 5

    return min(score, 100)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3 – CLERK PORTAL SCRAPING
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
    """Retry wrapper – swallows exceptions until last attempt."""
    for i in range(attempts):
        try:
            return fn()
        except Exception as exc:
            if i == attempts - 1:
                raise
            log.warning(f"Attempt {i+1} failed: {exc} – retrying in {delay}s")
            time.sleep(delay)


def _parse_amount(text: str) -> float:
    """Extract dollar amount from a string."""
    if not text:
        return 0.0
    cleaned = re.sub(r"[^\d.]", "", str(text))
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def _parse_date(text: str) -> str:
    """Normalise date to YYYY-MM-DD."""
    if not text:
        return ""
    text = text.strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return text


# ── Cook County Recorder of Deeds (ccrecorder.org) ───────────────────────────

def build_search_url(doc_type: str, date_from: str, date_to: str) -> str:
    """Build search URL for Cook County Recorder portal."""
    return (
        f"{CLERK_SEARCH}?DocType={doc_type}"
        f"&DateFrom={date_from}&DateTo={date_to}"
        f"&County=Cook&State=IL"
    )


async def scrape_clerk_playwright(doc_type: str, date_from: str, date_to: str) -> list[dict]:
    """
    Use Playwright to scrape Cook County Recorder for a specific doc type.
    Handles JavaScript-rendered pages and __doPostBack pagination.
    """
    records = []
    if not PLAYWRIGHT_AVAILABLE:
        return records

    url = build_search_url(doc_type, date_from, date_to)
    log.info(f"[PW] Scraping {doc_type}: {url}")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=HEADLESS)
        context = await browser.new_context(
            user_agent=HEADERS["User-Agent"],
            viewport={"width": 1280, "height": 800},
        )
        page = await context.new_page()

        try:
            await page.goto(url, timeout=30_000, wait_until="domcontentloaded")
            await page.wait_for_timeout(2000)

            # Handle search form if present
            try:
                await page.fill("input[name='DocType'], input[id*='DocType']", doc_type, timeout=3000)
                await page.fill("input[name='DateFrom'], input[id*='DateFrom']", date_from, timeout=3000)
                await page.fill("input[name='DateTo'], input[id*='DateTo']", date_to, timeout=3000)
                await page.click("input[type='submit'], button[type='submit']", timeout=3000)
                await page.wait_for_timeout(3000)
            except PWTimeout:
                pass  # May not need form interaction

            # Paginate through results
            page_num = 1
            while True:
                html = await page.content()
                page_records = _parse_clerk_html(html, doc_type)
                records.extend(page_records)
                log.info(f"  Page {page_num}: {len(page_records)} records")

                # Try next page via __doPostBack or Next link
                next_found = False
                try:
                    next_btn = page.locator(
                        "a:has-text('Next'), input[value='Next >'], "
                        "a[href*='__doPostBack']:has-text('>')"
                    ).first
                    if await next_btn.is_visible(timeout=2000):
                        await next_btn.click(timeout=5000)
                        await page.wait_for_timeout(2000)
                        next_found = True
                        page_num += 1
                except (PWTimeout, Exception):
                    pass

                if not next_found:
                    break

        except Exception as exc:
            log.warning(f"Playwright error for {doc_type}: {exc}")
        finally:
            await browser.close()

    return records


def scrape_clerk_requests(doc_type: str, date_from: str, date_to: str) -> list[dict]:
    """
    Fallback: use requests + BeautifulSoup to scrape the clerk portal.
    Handles static HTML pages and __doPostBack POST requests.
    """
    records = []
    url = build_search_url(doc_type, date_from, date_to)
    log.info(f"[REQ] Scraping {doc_type}: {url}")

    def fetch_page(target_url, data=None):
        if data:
            return SESSION.post(target_url, data=data, timeout=20)
        return SESSION.get(target_url, timeout=20)

    try:
        resp = _retry(lambda: fetch_page(url))
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        # Extract hidden form fields for pagination
        viewstate    = _get_hidden(soup, "__VIEWSTATE")
        eventval     = _get_hidden(soup, "__EVENTVALIDATION")
        viewstategr  = _get_hidden(soup, "__VIEWSTATEGENERATOR")

        page_num = 1
        while True:
            page_records = _parse_clerk_html(resp.text, doc_type)
            records.extend(page_records)
            log.info(f"  Page {page_num}: {len(page_records)} records")

            # Check for next page link
            next_link = soup.find("a", string=re.compile(r"Next|>", re.I))
            if not next_link:
                break

            # Handle __doPostBack
            href = next_link.get("href", "")
            post_match = re.search(r"__doPostBack\('([^']+)','([^']*)'\)", href)
            if post_match:
                target, arg = post_match.groups()
                post_data = {
                    "__EVENTTARGET":       target,
                    "__EVENTARGUMENT":     arg,
                    "__VIEWSTATE":         viewstate,
                    "__EVENTVALIDATION":   eventval,
                    "__VIEWSTATEGENERATOR": viewstategr,
                }
                resp = _retry(lambda: fetch_page(url, post_data))
                resp.raise_for_status()
                soup = BeautifulSoup(resp.text, "lxml")
                # Refresh hidden fields
                viewstate    = _get_hidden(soup, "__VIEWSTATE")
                eventval     = _get_hidden(soup, "__EVENTVALIDATION")
                viewstategr  = _get_hidden(soup, "__VIEWSTATEGENERATOR")
                page_num += 1
            else:
                break

    except Exception as exc:
        log.warning(f"Requests scrape error for {doc_type}: {exc}")

    return records


def _get_hidden(soup: BeautifulSoup, name: str) -> str:
    tag = soup.find("input", {"name": name})
    return tag["value"] if tag and tag.get("value") else ""


def _parse_clerk_html(html: str, doc_type: str) -> list[dict]:
    """
    Parse Cook County Recorder search results HTML.
    Tries multiple table structures for robustness.
    """
    records = []
    soup = BeautifulSoup(html, "lxml")
    cat, cat_label = DOC_TYPE_MAP.get(doc_type, (doc_type, doc_type))

    # Look for result rows in common table patterns
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

        # Skip header rows
        if any(h in texts[0].lower() for h in ("doc", "instrument", "type", "filed", "#")):
            if "filed" in " ".join(texts).lower():
                continue

        try:
            doc_num, filed, grantor, grantee, amount_str, legal = _extract_cells(texts)
            if not doc_num:
                continue

            # Build clerk URL from document number
            clerk_url = f"{CLERK_BASE}/Search/DocDisplay?DocNum={doc_num}"

            record = {
                "doc_num":   doc_num,
                "doc_type":  doc_type,
                "filed":     _parse_date(filed),
                "cat":       cat,
                "cat_label": cat_label,
                "owner":     _clean(grantor),
                "grantee":   _clean(grantee),
                "amount":    _parse_amount(amount_str),
                "legal":     _clean(legal),
                "clerk_url": clerk_url,
            }
            records.append(record)
        except Exception:
            continue

    # Also try JSON embedded in page (some portals return JSON)
    records.extend(_parse_embedded_json(html, doc_type, cat, cat_label))

    return records


def _extract_cells(texts: list[str]) -> tuple:
    """
    Heuristically extract fields from table row.
    Returns: (doc_num, filed, grantor, grantee, amount, legal)
    """
    # Common column orders for Cook County recorder
    if len(texts) >= 6:
        return texts[0], texts[1], texts[2], texts[3], texts[4], texts[5]
    elif len(texts) == 5:
        return texts[0], texts[1], texts[2], texts[3], texts[4], ""
    elif len(texts) == 4:
        return texts[0], texts[1], texts[2], texts[3], "", ""
    else:
        return texts[0], texts[1] if len(texts) > 1 else "", "", "", "", ""


def _parse_embedded_json(html: str, doc_type: str, cat: str, cat_label: str) -> list[dict]:
    """Extract records from JSON embedded in page (AJAX-loaded portals)."""
    records = []
    # Find JSON arrays in script tags
    matches = re.findall(r'\[(\{["\']doc["\'].*?\})\]', html, re.DOTALL)
    for match in matches:
        try:
            data = json.loads(f"[{match}]")
            for item in data:
                if isinstance(item, dict):
                    records.append({
                        "doc_num":   str(item.get("doc", item.get("DocNum", ""))),
                        "doc_type":  doc_type,
                        "filed":     _parse_date(str(item.get("filed", item.get("FiledDate", "")))),
                        "cat":       cat,
                        "cat_label": cat_label,
                        "owner":     _clean(str(item.get("grantor", item.get("Grantor", "")))),
                        "grantee":   _clean(str(item.get("grantee", item.get("Grantee", "")))),
                        "amount":    _parse_amount(str(item.get("amount", "0"))),
                        "legal":     _clean(str(item.get("legal", ""))),
                        "clerk_url": f"{CLERK_BASE}/Search/DocDisplay?DocNum={item.get('doc', '')}",
                    })
        except (json.JSONDecodeError, Exception):
            pass
    return records


def _clean(text: str) -> str:
    if not text:
        return ""
    return " ".join(str(text).split())


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 4 – RECORD ENRICHMENT & DEDUP
# ══════════════════════════════════════════════════════════════════════════════

def enrich_record(record: dict, parcel_lookup: dict) -> dict:
    """Add parcel address data, flags, and score to a record."""
    parcel = lookup_parcel(record.get("owner", ""), parcel_lookup)

    enriched = {**record}
    enriched.update({
        "prop_address": parcel.get("prop_address", ""),
        "prop_city":    parcel.get("prop_city", ""),
        "prop_state":   parcel.get("prop_state", "IL"),
        "prop_zip":     parcel.get("prop_zip", ""),
        "mail_address": parcel.get("mail_address", ""),
        "mail_city":    parcel.get("mail_city", ""),
        "mail_state":   parcel.get("mail_state", "IL"),
        "mail_zip":     parcel.get("mail_zip", ""),
    })

    flags             = compute_flags(enriched)
    enriched["flags"] = flags
    enriched["score"] = compute_score(enriched, flags)
    return enriched


def deduplicate(records: list[dict]) -> list[dict]:
    seen = set()
    out  = []
    for r in records:
        key = (r.get("doc_num", ""), r.get("doc_type", ""))
        if key not in seen and key[0]:
            seen.add(key)
            out.append(r)
    return out


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 5 – OUTPUT WRITERS
# ══════════════════════════════════════════════════════════════════════════════

RECORD_SCHEMA = [
    "doc_num", "doc_type", "filed", "cat", "cat_label",
    "owner", "grantee", "amount", "legal",
    "prop_address", "prop_city", "prop_state", "prop_zip",
    "mail_address", "mail_city", "mail_state", "mail_zip",
    "clerk_url", "flags", "score",
]


def normalise_record(r: dict) -> dict:
    """Ensure all schema fields exist with sensible defaults."""
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


def save_json(records: list[dict], date_from: str, date_to: str) -> None:
    DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    with_address = sum(1 for r in records if r.get("prop_address"))

    payload = {
        "fetched_at":  datetime.utcnow().isoformat() + "Z",
        "source":      "Cook County Recorder of Deeds",
        "date_range":  {"from": date_from, "to": date_to},
        "total":       len(records),
        "with_address": with_address,
        "records":     [normalise_record(r) for r in records],
    }

    for path in [DASHBOARD_DIR / "records.json", DATA_DIR / "records.json"]:
        path.write_text(json.dumps(payload, indent=2, default=str))
        log.info(f"JSON saved → {path}")


def save_ghl_csv(records: list[dict]) -> None:
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
            owner  = r.get("owner", "")
            parts  = owner.split()
            first  = parts[0] if parts else ""
            last   = " ".join(parts[1:]) if len(parts) > 1 else ""

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
# SECTION 6 – MAIN ORCHESTRATOR
# ══════════════════════════════════════════════════════════════════════════════

async def run_playwright_scrape(doc_type: str, date_from: str, date_to: str) -> list[dict]:
    try:
        return await scrape_clerk_playwright(doc_type, date_from, date_to)
    except Exception as exc:
        log.warning(f"Playwright failed for {doc_type}: {exc}")
        return []


async def main() -> None:
    log.info("=" * 60)
    log.info("Cook County Motivated Seller Lead Scraper")
    log.info("=" * 60)

    date_to   = datetime.utcnow().strftime("%m/%d/%Y")
    date_from = (datetime.utcnow() - timedelta(days=LOOKBACK_DAYS)).strftime("%m/%d/%Y")
    log.info(f"Date range: {date_from} → {date_to}")

    # ── 1. Load parcel data ───────────────────────────────────────────────────
    log.info("Loading parcel data …")
    parcel_lookup = download_parcel_data()
    log.info(f"Parcel lookup ready – {len(parcel_lookup):,} entries")

    # ── 2. Scrape each document type ─────────────────────────────────────────
    all_records: list[dict] = []

    for doc_type in TARGET_TYPES:
        log.info(f"\n── Fetching: {doc_type} ──")
        try:
            # Try Playwright first, fall back to requests
            if PLAYWRIGHT_AVAILABLE:
                records = await run_playwright_scrape(doc_type, date_from, date_to)
            else:
                records = []

            if not records:
                records = scrape_clerk_requests(doc_type, date_from, date_to)

            log.info(f"  → {len(records)} raw records for {doc_type}")
            all_records.extend(records)

        except Exception as exc:
            log.error(f"Fatal error scraping {doc_type}: {exc}")
            log.debug(traceback.format_exc())

    # ── 3. Deduplicate ────────────────────────────────────────────────────────
    log.info(f"\nDeduplicating {len(all_records)} raw records …")
    all_records = deduplicate(all_records)
    log.info(f"After dedup: {len(all_records)} unique records")

    # ── 4. Enrich with parcel data + scoring ──────────────────────────────────
    log.info("Enriching records with parcel data and computing scores …")
    enriched = []
    for r in all_records:
        try:
            enriched.append(enrich_record(r, parcel_lookup))
        except Exception as exc:
            log.warning(f"Enrich failed for {r.get('doc_num')}: {exc}")

    # Sort by score desc
    enriched.sort(key=lambda r: r.get("score", 0), reverse=True)

    with_addr = sum(1 for r in enriched if r.get("prop_address"))
    log.info(f"Enriched: {len(enriched)} records, {with_addr} with address")

    # ── 5. Save outputs ────────────────────────────────────────────────────────
    log.info("\nSaving outputs …")
    save_json(enriched, date_from, date_to)
    save_ghl_csv(enriched)

    # ── 6. Summary ────────────────────────────────────────────────────────────
    log.info("\n" + "=" * 60)
    log.info(f"COMPLETE: {len(enriched)} leads captured")
    log.info(f"  With address:  {with_addr}")
    log.info(f"  Score ≥ 70:    {sum(1 for r in enriched if r.get('score', 0) >= 70)}")
    log.info(f"  Score ≥ 50:    {sum(1 for r in enriched if r.get('score', 0) >= 50)}")

    # Breakdown by category
    from collections import Counter
    cats = Counter(r.get("cat_label", "Unknown") for r in enriched)
    log.info("\nBy type:")
    for label, count in cats.most_common():
        log.info(f"  {label:<30} {count:>5}")
    log.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
