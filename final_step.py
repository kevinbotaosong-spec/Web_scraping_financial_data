from main import process_company
from first_01 import fetch_company_tickers
import time
import pandas as pd

# 获取所有公司列表
df_companies = fetch_company_tickers()
print(f"总共 {len(df_companies)} 家公司")

# 批量处理
results = []
failed = []

for idx, row in df_companies.iterrows():
    ticker = row['ticker']
    cik = row['cik']
    
    try:
        print(f"\n[{idx+1}/{len(df_companies)}] 处理 {ticker} ({row['company_name']})")
        result = process_company(ticker, years_back=5, save_to_csv=True)
        results.append({
            'ticker': ticker,
            'cik': cik,
            'company_name': row['company_name'],
            'status': 'success',
            'financial_records': len(result['financial']),
            'filings_count': len(result['filings'])
        })
        print(f"✅ {ticker} 完成")
        
    except Exception as e:
        print(f"❌ {ticker} 失败: {e}")
        failed.append({
            'ticker': ticker,
            'cik': cik,
            'company_name': row['company_name'],
            'error': str(e)
        })
    
    # 添加延迟，避免请求过快
    time.sleep(0.1)

# 保存处理结果
df_results = pd.DataFrame(results)
df_results.to_csv("batch_processing_results.csv", index=False)

df_failed = pd.DataFrame(failed)
df_failed.to_csv("batch_processing_failed.csv", index=False)

print(f"\n处理完成！")
print(f"成功: {len(results)} 家")
print(f"失败: {len(failed)} 家")