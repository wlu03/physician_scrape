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
    TEMPLATE — inspect the live page and update selectors accordingly.
    """
    # search page
    search_url = f"https://www.healthgrades.com/usearch?what={full_name.replace(' ', '+')}"
    result = {
        "hg_url":            None,
        "hg_rating":         None,
        "hg_review_count":   None,
        "hg_education":      None,
        "hg_hospital_affil": None,
    }
    try:
        resp = requests.get(search_url, headers=HEADERS, timeout=12)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # update these CSS selectors after inspecting the live page 
        first_result = soup.select_one("a.provider-name-link")   # example selector
        if not first_result:
            return result

        profile_path = first_result.get("href", "")
        profile_url  = "https://www.healthgrades.com" + profile_path
        result["hg_url"] = profile_url

        time.sleep(REQUEST_DELAY)
        profile_resp = requests.get(profile_url, headers=HEADERS, timeout=12)
        profile_resp.raise_for_status()
        profile_soup = BeautifulSoup(profile_resp.text, "html.parser")

        # update these selectors to match actual page structure
        rating_tag   = profile_soup.select_one("[data-testid='rating-value']")
        reviews_tag  = profile_soup.select_one("[data-testid='review-count']")
        edu_tag      = profile_soup.select_one(".education-training-item")
        hospital_tag = profile_soup.select_one(".hospital-affiliation-name")

        result["hg_rating"] = rating_tag.get_text(strip=True)   if rating_tag   else None
        result["hg_review_count"]  = reviews_tag.get_text(strip=True)  if reviews_tag  else None
        result["hg_education"] = edu_tag.get_text(strip=True)      if edu_tag      else None
        result["hg_hospital_affil"] = hospital_tag.get_text(strip=True) if hospital_tag else None

    except requests.RequestException as exc:
        log.warning("Healthgrades error for %s: %s", full_name, exc)
    except Exception as exc:
        log.warning("Healthgrades parse error for %s: %s", full_name, exc)

    return result


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
        best = npi_records[0]   # take top match, could rank further if needed
        row.update(parse_npi_record(best))
        if len(npi_records) > 1:
            row["npi_match_count"] = len(npi_records)
            row["npi_note"] = "Multiple NPI matches found — review manually"
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
