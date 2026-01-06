"""
SEC财务数据爬虫主程序
整合任务1和任务2的所有功能
"""
import pandas as pd
from first_01 import fetch_company_tickers
from task1_filings import get_filings_for_company
from task1_financial_data import get_financial_data_for_company
from task2_segment_geo import get_segment_geographic_data, validate_segment_geo_data
#新建 main.py - 主程序

def process_company(ticker, years_back=5, save_to_csv=True):
    """
    处理单个公司的完整数据抓取流程
    
    Args:
        ticker: 股票代码
        years_back: 回溯年数
        save_to_csv: 是否保存为CSV文件
    """
    print(f"\n{'='*60}")
    print(f"Processing {ticker}")
    print(f"{'='*60}")
    
    # 1. 获取公司列表
    df_companies = fetch_company_tickers()
    company_row = df_companies[df_companies["ticker"] == ticker]
    if len(company_row) == 0:
        print(f"Error: Ticker {ticker} not found")
        return
    
    cik = company_row.iloc[0]["cik"]
    company_name = company_row.iloc[0]["company_name"]
    print(f"Company: {company_name}")
    print(f"CIK: {cik}")
    
    # 2. 获取filings列表
    print("\n[Step 1] Fetching filings...")
    df_filings = get_filings_for_company(cik, years_back)
    print(f"Found {len(df_filings)} filings (10-K and 10-Q)")
    
    # 3. 获取财务数据
    print("\n[Step 2] Fetching financial data...")
    df_financial, quality_report = get_financial_data_for_company(cik, years_back)
    
    # 将quality_report转换为结构化的DataFrame
    quality_records = []
    for check in quality_report.get("checks", []):
        quality_records.append({"type": "check", "message": check})
    for warning in quality_report.get("warnings", []):
        quality_records.append({"type": "warning", "message": warning})
    for error in quality_report.get("errors", []):
        quality_records.append({"type": "error", "message": error})
    
    df_quality_report = pd.DataFrame(quality_records) if quality_records else pd.DataFrame(columns=["type", "message"])
    
    print(f"Found {len(df_financial)} financial data points")
    print(f"Quality checks: {len(quality_report['checks'])} passed, "
          f"{len(quality_report['warnings'])} warnings")
    
    # # 4. 获取segment和geographic数据
    # print("\n[Step 3] Fetching segment and geographic data...")
    # df_segment, df_geo, missing = get_segment_geographic_data(cik, years_back)
    # print(f"Segment data: {len(df_segment)} records")
    # print(f"Geographic data: {len(df_geo)} records")
    # print(f"Missing reports: {len(missing)}")
    #
    # # 5. 验证数据
    # print("\n[Step 4] Validating data...")
    # validation = validate_segment_geo_data(df_segment, df_geo, df_financial)
    # print(f"Validation checks: {len(validation['segment_checks']) + len(validation['geo_checks'])} passed")
    # if validation["warnings"]:
    #     print("Warnings:")
    #     for w in validation["warnings"]:
    #         print(f"  - {w}")
    
    # 6. 保存数据
    if save_to_csv:
        prefix = f"{ticker}_{cik}"
        df_filings.to_csv(f"{prefix}_filings.csv", index=False)
        df_financial.to_csv(f"{prefix}_financial.csv", index=False)
        df_quality_report.to_csv(f"{prefix}_quality_report.csv", index=False)
        # if len(df_segment) > 0:
        #     df_segment.to_csv(f"{prefix}_segment.csv", index=False)
        # if len(df_geo) > 0:
        #     df_geo.to_csv(f"{prefix}_geographic.csv", index=False)
        print(f"\n[Step 5] Data saved to CSV files with prefix: {prefix}")
    
    return {
        "ticker": ticker,
        "cik": cik,
        "company_name": company_name,
        "filings": df_filings,
        "financial": df_financial,
        # "segment": df_segment,
        # "geographic": df_geo,
        "quality_report": quality_report,
        #"validation": validation,
    }

if __name__ == "__main__":
    # 示例：处理AAPL
    result = process_company("AAPL", years_back=5)
    print(result)
    # 可以批量处理多个公司
    # tickers = ["AAPL", "MSFT", "GOOGL"]
    # for ticker in tickers:
    #     process_company(ticker, years_back=5)
