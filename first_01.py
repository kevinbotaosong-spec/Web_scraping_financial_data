import requests
import json
import pandas as pd
from datetime import datetime, timedelta

def get_sec_headers():
    """返回SEC要求的User-Agent"""
    return {
        "User-Agent": "Kevin-Research (kevinbotaosong@ucla.edu)"
    }


def fetch_company_tickers(url="https://www.sec.gov/files/company_tickers.json"):
    """获取所有公司的ticker和CIK映射"""
    resp = requests.get(url, headers=get_sec_headers())
    resp.raise_for_status()
    data = resp.json()
    
    records = []
    for record in data.values():
        rec = {
            "company_name": record.get("title"),
            "ticker": record.get("ticker"),
            "cik": str(record.get("cik_str")).rjust(10, "0"),
            #"mainfinancedate": f"https://data.sec.gov/api/xbrl/companyfacts/CIK{str(record.get("cik_str")).rjust(10, "0")}.json"
        }
        records.append(rec)
    
    df = pd.DataFrame(records)
    return df #(this will be de_companies)

if __name__ == "__main__":
    df = fetch_company_tickers()
    print(f"Total companies: {len(df)}")
    print(df.head())
    #df.to_csv("whatsup.csv")
