# Physician Profile Scraper

This project takes a list of physician names, looks them up in the CMS NPI Registry, optionally scrapes Healthgrades, and exports the results to an Excel file.

## Files

```
physician_scraper/
├── scraper.py
├── requirements.txt
├── physician_names.txt
└── physicians_database.xlsx
```

## Setup

Install dependencies:

```bash
pip install -r requirements.txt
```

Default run:

```bash
python scraper.py
```

Custom input/output:

```bash
python scraper.py --input my_list.txt --output results.xlsx
```

Skip website scraping:

```bash
python scraper.py --no-web
```

## Input

Put one physician name per line in `physician_names.txt`:

```
John Smith
Maria Garcia
James A. Johnson
```

## Output

The script creates an Excel file with one row per physician, including NPI details and, if enabled, website data such as ratings or affiliations.

## Notes

- The CMS NPI Registry API is public and free to use.
- Website scraping may need selector updates if page structure changes.
- Close the Excel file before re-running the script.