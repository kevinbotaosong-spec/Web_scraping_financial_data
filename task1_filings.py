import requests
import json
import pandas as pd
from datetime import datetime, timedelta
from first_01 import get_sec_headers
#新建 task1_filings.py - 抓取10-K/10-Q文件列表
def get_filings_for_company(cik, years_back=5): #要确认cik
    """
    获取公司过去N年的10-K和10-Q文件列表

    Args:
        cik: 10位CIK字符串，如 "0000320193"
        years_back: 回溯年数，默认5年
    
    Returns:
        DataFrame包含所有10-K和10-Q文件信息
        根据日期由近到远，由(由10K、Q等)cik，form，date，doc，url 组成
    """
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    resp = requests.get(url, headers=get_sec_headers())
    resp.raise_for_status()
    data = resp.json()
    
    # 获取filings.recent部分
    recent = data.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    filing_dates = recent.get("filingDate", [])
    primary_docs = recent.get("primaryDocument", [])
    accession_numbers = recent.get("accessionNumber", [])
    
    # 计算截止日期（5年前）
    cutoff_date = datetime.now() - timedelta(days=years_back * 365) #这里需要考虑闰年吗（小细节）
    
    filings = []
    for i in range(len(forms)):
        form = forms[i]
        # 只关注10-K和10-Q（包括修正版）
        if form in ["10-K", "10-K/A", "10-Q", "10-Q/A"]:
            filing_date = datetime.strptime(filing_dates[i], "%Y-%m-%d")
            
            # 只保留过去5年的
            if filing_date >= cutoff_date:
                accession_no = accession_numbers[i]
                primary_doc = primary_docs[i]

                
                # 构建真实URL
                cik_numeric = str(int(cik))  # 去掉前导零  （这里放在for循环外面会不会减少运算量）
                accession_no_clean = accession_no.replace("-", "")
                filing_url = (
                    f"https://www.sec.gov/Archives/edgar/data/"
                    f"{cik_numeric}/{accession_no_clean}/{primary_doc}"
                )
                
                filings.append({
                    "cik": cik,  # 这个cik是已经补好的10位数字
                    "form": form,
                    "filing_date": filing_dates[i],
                    "accession_number": accession_no,
                    "primary_document": primary_doc,
                    "filing_url": filing_url,
                    "fiscal_year": None,  # 后续从companyfacts获取
                })
    
    df = pd.DataFrame(filings)
    if len(df) > 0:
        df = df.sort_values("filing_date", ascending=False)
    
    return df  # 根据日期由近到远，由(由10K、Q等)cik，form，date，doc，url 组成

def get_all_filings_for_ticker(ticker, df_companies, years_back=5):
    """
    根据ticker获取所有filings
    
    Args:
        ticker: 股票代码，如 "AAPL"
        df_companies: 公司列表DataFrame
        years_back: 回溯年数
    return:
        这里返回的表格的链接最红可以直接看10K等文件，html格式
    """
    company_row = df_companies[df_companies["ticker"] == ticker]
    #这里是ticker 还是tickers，根据输入的ticker(股票代码)找公司
    if len(company_row) == 0:
        raise ValueError(f"Ticker {ticker} not found")
    
    cik = company_row.iloc[0]["cik"]
    return get_filings_for_company(cik, years_back)
#这里返回的表格的链接最红可以直接看10K等文件，html格式
if __name__ == "__main__":
    # 测试：获取AAPL的filings
    from first_01 import fetch_company_tickers
    
    df_companies = fetch_company_tickers()
    df_filings = get_all_filings_for_ticker("AAPL", df_companies, years_back=5)
    print(f"\nFound {len(df_filings)} filings for AAPL:")
    print(df_filings[["form", "filing_date", "filing_url"]].head(10))
    df2=df_filings[["form", "filing_date", "filing_url"]].head(10)
    #df2.to_csv("df_filing.csv")
