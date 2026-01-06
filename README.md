📊 SEC Financial Data Crawler & Analyzer

A Python-based data pipeline that collects, standardizes, and validates financial statement data for U.S. public companies using official SEC (EDGAR) APIs.

This project extracts 10-K / 10-Q filings, core financial metrics, and segment & geographic revenue disclosures, and performs automated data quality checks suitable for financial analysis and research.

🚀 Project Overview

This project is designed to build a reliable financial dataset directly from the U.S. Securities and Exchange Commission (SEC), with a focus on:

Structured XBRL financial data

Filing metadata and URLs

Cash flow and balance sheet consistency

Business segment and geographic revenue disclosures

It supports single-company analysis as well as large-scale batch processing.

📌 Key Features
1️⃣ Core Financial Data Extraction

Fetches financial data from SEC companyfacts XBRL API

Extracts key metrics:

Revenue

Operating Income

Net Income

EPS

Assets, Liabilities, Equity

Operating / Investing / Financing Cash Flow

Capital Expenditures

Cash & Cash Equivalents

Supports both annual (10-K) and quarterly (10-Q) filings

2️⃣ Filing Metadata Collection

Retrieves all 10-K and 10-Q filings within a configurable time window

Generates direct-access HTML URLs for each filing

Stores accession numbers, filing dates, and document names

3️⃣ Automated Data Quality Checks

Two financial consistency checks are implemented:

✅ Balance Sheet Check
Assets ≈ Liabilities + Equity


Annual data only

Configurable tolerance (default: 1%)

✅ Cash Flow Check
Operating CF + Investing CF + Financing CF ≈ Δ Cash


Annual data only

First year automatically skipped

Configurable tolerance (default: 1%)

All results are summarized in a structured quality report with:

checks

warnings

errors

4️⃣ Segment & Geographic Revenue Extraction

Parses 10-K HTML filings

Extracts:

Business segment revenue

Geographic revenue breakdown

Validates:

Segment / Geographic total ≈ Reported revenue (≤ 3% deviation)


⚠️ Not all companies disclose segment or geographic data.

🗂 Project Structure
.
├── first_01.py                # Fetches company tickers & CIK mappings
├── task1_filings.py           # Retrieves 10-K / 10-Q filing metadata
├── task1_financial_data.py    # Extracts and validates financial metrics
├── task2_segment_geo.py       # Parses segment & geographic revenue tables
├── main.py                    # End-to-end pipeline orchestration
├── batch_processing_results.csv
├── batch_processing_failed.csv
└── README.md

🔧 Installation
pip install pandas requests beautifulsoup4 lxml


Python version: 3.9+ recommended

▶️ Usage
🔹 Process a Single Company
from main import process_company

result = process_company("AAPL", years_back=5)

print(result["company_name"])
print(len(result["financial"]))
print(result["quality_report"])

🔹 Batch Process All Companies
from first_01 import fetch_company_tickers
from main import process_company
import time

df = fetch_company_tickers()

for _, row in df.iterrows():
    try:
        process_company(row["ticker"], years_back=5)
        time.sleep(0.1)  # Respect SEC rate limits
    except Exception as e:
        print(f"Failed: {row['ticker']}, {e}")

📤 Output Files

All outputs follow the naming convention:

{TICKER}_{CIK}_{type}.csv

Generated Files:

*_filings.csv – Filing metadata

*_financial.csv – Financial time-series data

*_quality_report.csv – Data validation results

*_segment.csv – Segment revenue (if available)

*_geographic.csv – Geographic revenue (if available)

🌐 Data Sources

All data is obtained from official SEC endpoints:

Company list
https://www.sec.gov/files/company_tickers.json

Filing submissions
https://data.sec.gov/submissions/CIK{CIK}.json

XBRL financial data
https://data.sec.gov/api/xbrl/companyfacts/CIK{CIK}.json

Filing HTML documents
https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/{filename}

⏱ Performance & Limitations

Recommended request rate: ≤ 10 requests / second

Processing time:

~5–10 seconds per company

~2–3 hours for 1,000 companies

Not all companies have complete financial or segment data

HTML parsing may fail for non-standard filing formats

🧠 Use Cases

Financial statement analysis

DCF / valuation modeling

Accounting quality research

Factor investing datasets

Machine learning feature engineering

Academic finance research

⚠️ Disclaimer

This project is for educational and research purposes only.
Data accuracy depends on SEC filings and issuer disclosures.
