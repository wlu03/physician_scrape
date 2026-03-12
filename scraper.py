import argparse
import logging
import time
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# Constants
NPI_API_URL = "https://npiregistry.cms.hhs.gov/api/"
REQUEST_DELAY = 1.5   # seconds between requests
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; PhysicianResearchBot/1.0; "
        "+https://economics.gatech.edu)"
    )
}


# NPI Registry (free, no login required, JSON API)
def query_npi_registry(first_name: str, last_name: str) -> list[dict]:
    """
    Query the CMS NPI Registry for a physician by name.
    Returns a list of matching records (there may be more than one match).
    """
    params = {
        "version": "2.1",
        "first_name": first_name,
        "last_name": last_name,
        "enumeration_type": "NPI-1",   # individual providers only
        "limit": 5,
    }
    try:
        resp = requests.get(NPI_API_URL, params=params, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        return data.get("results", [])
    except requests.RequestException as exc:
        log.warning("NPI API error for %s %s: %s", first_name, last_name, exc)
        return []


def parse_npi_record(record: dict) -> dict:
    """
    Flatten one NPI JSON record into a plain dict with human-readable keys.
    """
    basic = record.get("basic", {})
    taxonomies = record.get("taxonomies", [{}])
    primary_tax = next((t for t in taxonomies if t.get("primary")), taxonomies[0] if taxonomies else {})
    addresses = record.get("addresses", [{}])
    practice_addr = next((a for a in addresses if a.get("address_purpose") == "LOCATION"), addresses[0] if addresses else {})

    return {
        "npi_number":          record.get("number"),
        "first_name":          basic.get("first_name"),
        "last_name":           basic.get("last_name"),
        "middle_name":         basic.get("middle_name"),
        "credential":          basic.get("credential"),
        "gender":              basic.get("gender"),
        "enumeration_date":    basic.get("enumeration_date"),
        "last_updated":        basic.get("last_updated"),
        "status":              basic.get("status"),
        "sole_proprietor":     basic.get("sole_proprietor"),
        "specialty":           primary_tax.get("desc"),
        "specialty_code":      primary_tax.get("code"),
        "license_number":      primary_tax.get("license"),
        "license_state":       primary_tax.get("state"),
        "practice_address_1":  practice_addr.get("address_1"),
        "practice_address_2":  practice_addr.get("address_2"),
        "practice_city":       practice_addr.get("city"),
        "practice_state":      practice_addr.get("state"),
        "practice_zip":        practice_addr.get("postal_code"),
        "practice_phone":      practice_addr.get("telephone_number"),
        "practice_fax":        practice_addr.get("fax_number"),
    }


# Healthgrades (HTML scraping template)
def scrape_healthgrades(full_name: str) -> dict:
    """
    Scrape Healthgrades profile page for a physician.

    NOTE: Healthgrades is a fully JavaScript-rendered (React) site.
    requests + BeautifulSoup only receives the raw JS bundle — no doctor
    data is present in the initial HTML response.  To make this work you
    need a headless browser such as Selenium or Playwright, e.g.:

        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page()
            page.goto(search_url)
            page.wait_for_selector("a.provider-name-link")
            html = page.content()
        soup = BeautifulSoup(html, "html.parser")

    Until browser automation is added, this function returns empty fields.
    """
    return {
        "hg_url":            None,
        "hg_rating":         None,
        "hg_review_count":   None,
        "hg_education":      None,
        "hg_hospital_affil": None,
    }


# STUB for additional website
# Copy this pattern and fill in the URL logic + BeautifulSoup selectors.
def scrape_site_b(full_name: str) -> dict:
    """
    Placeholder for a second custom website.
    Returns empty dict until implemented.
    """
    return {
        "siteb_field_1": None,
        "siteb_field_2": None,
    }


# Orchestrator
def _name_matches(record: dict, first: str, last: str) -> bool:
    """Return True if the NPI record's name matches the queried first/last name."""
    basic = record.get("basic", {})
    rec_first = (basic.get("first_name") or "").lower()
    rec_last  = (basic.get("last_name")  or "").lower()
    return rec_first == first.lower() and rec_last == last.lower()


def _pick_best_npi(records: list[dict], first: str, last: str) -> tuple[dict, str]:
    """
    Return (best_record, note).
    Prefers exact name matches; falls back to first result with a warning.
    """
    exact = [r for r in records if _name_matches(r, first, last)]
    if exact:
        note = f"{len(exact)} exact NPI match(es) found" if len(exact) > 1 else ""
        return exact[0], note
    # No exact match — the API returned phonetic/partial results
    return records[0], "No exact name match in NPI results — top result used, review manually"


def process_physician(full_name: str, scrape_web: bool = True) -> dict:
    """
    Given a physician's full name, collect all available data and return
    a single merged dict representing one row in the output Excel file.
    """
    full_name = full_name.strip()
    parts = full_name.split()
    first = parts[0] if parts else ""
    last  = parts[-1] if len(parts) > 1 else ""

    log.info("Processing: %s", full_name)
    row = {"input_name": full_name}

    # NPI Registry
    npi_records = query_npi_registry(first, last)
    if npi_records:
        best, note = _pick_best_npi(npi_records, first, last)
        row.update(parse_npi_record(best))
        row["npi_match_count"] = len(npi_records)
        if note:
            row["npi_note"] = note
    else:
        row["npi_note"] = "No NPI record found"

    time.sleep(REQUEST_DELAY)

    if scrape_web:
        hg_data = scrape_healthgrades(full_name)
        row.update(hg_data)
        time.sleep(REQUEST_DELAY)

        siteb_data = scrape_site_b(full_name)
        row.update(siteb_data)

    return row


# Excel export
def export_to_excel(records: list[dict], output_path: str) -> None:

    df = pd.DataFrame(records)
    cols = ["input_name"] + sorted([c for c in df.columns if c != "input_name"])
    df = df[cols]

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Physicians")
        ws = writer.sheets["Physicians"]

        # Auto-fit column widths
        for col_cells in ws.columns:
            max_len = max((len(str(c.value or "")) for c in col_cells), default=10)
            ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 4, 50)

    log.info("Saved %d rows → %s", len(records), output_path)


def main():
    parser = argparse.ArgumentParser(description="Physician Profile Scraper")
    parser.add_argument("--input",     default="physician_names.txt",
                        help="Path to input file (one name per line)")
    parser.add_argument("--output",    default="physicians_database.xlsx",
                        help="Path for the output Excel file")
    parser.add_argument("--no-web",    action="store_true",
                        help="Skip website scraping; only use NPI Registry")
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        log.error("Input file not found: %s", input_path)
        raise SystemExit(1)

    names = [n for n in input_path.read_text().splitlines() if n.strip()]
    log.info("Loaded %d names from %s", len(names), input_path)

    records = []
    for name in names:
        try:
            row = process_physician(name, scrape_web=not args.no_web)
            records.append(row)
        except Exception as exc:
            log.error("Unexpected error for %s: %s", name, exc)
            records.append({"input_name": name, "error": str(exc)})

    export_to_excel(records, args.output)
    print(f"\nDone! Output saved to: {args.output}")


if __name__ == "__main__":
    main()
