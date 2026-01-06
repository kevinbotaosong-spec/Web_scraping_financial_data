import requests
import json
import pandas as pd
from datetime import datetime, timedelta
from first_01 import get_sec_headers
#新建 task1_financial_data.py - 抓取财务数据

#asset quality check 在这个文件里设定为1%，目前现金流还没有算
asset_quality_threshold = 1
cashflow_quality_threshold = 1  # 现金流检查的百分比阈值（%）

# 财务指标映射（US-GAAP标签）
FINANCIAL_METRICS = {
    "revenue": "Revenues",
    "operating_income": "OperatingIncomeLoss",
    "net_income": "NetIncomeLoss",
    "eps": "EarningsPerShareBasic",
    "assets": "Assets",
    "liabilities": "Liabilities",
    "equity": "Equity",
    "ocf": "NetCashProvidedByUsedInOperatingActivities",  # Operating Cash Flow
    "icf": "NetCashProvidedByUsedInInvestingActivities",
    "fcf": "NetCashProvidedByUsedInFinancingActivities",
    "capex": "CapitalExpenditures",
    "cash": "CashAndCashEquivalentsAtCarryingValue",
}
#备用的metrics:
# FINANCIAL_METRICS = {
#     "revenue": [
#         "Revenues",
#         "SalesRevenueNet",
#         "OperatingRevenue",
#         "RevenueFromContractWithCustomerExcludingAssessedTax",
#     ],
#     "operating_income": [
#         "OperatingIncomeLoss",
#         "OperatingProfit",
#     ],
#     "net_income": [
#         "NetIncomeLoss",
#         "ProfitLoss",
#         "NetIncomeLossAvailableToCommonStockholdersBasic",
#     ],
#     "eps": [
#         "EarningsPerShareBasic",
#         "EarningsPerShareBasicAndDiluted",
#         "EarningsPerShareDiluted",
#     ],
#     "assets": ["Assets"],
#     "liabilities": ["Liabilities"],
#     "equity": ["Equity"],
#     "ocf": [
#         "NetCashProvidedByUsedInOperatingActivities",
#         "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations",
#     ],
#     "capex": [
#         "CapitalExpenditures",
#         "PaymentsToAcquirePropertyPlantAndEquipment",
#     ],
#     "cash": [
#         "CashAndCashEquivalentsAtCarryingValue",
#         "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
#     ],
# }


def fetch_company_facts(cik):
    """
    获取公司的companyfacts JSON数据
    
    Args:
        cik: 10位CIK字符串 (需要输入补好的)
    
    Returns:
        完整的companyfacts JSON数据
    """
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    resp = requests.get(url, headers=get_sec_headers())
    resp.raise_for_status()
    return resp.json()

def extract_financial_data(company_facts, years_back=5):
    """
    从companyfacts JSON中提取标准化的财务数据
    
    Args:
        company_facts: companyfacts JSON数据
        years_back: 回溯年数
    
    Returns:
        DataFrame包含所有财务指标
    """
    cutoff_date = datetime.now() - timedelta(days=years_back * 365)
    
    facts = company_facts.get("facts", {}).get("us-gaap", {}) #只有us-gaap这一栏有用，Document and Entity Information等不需要考虑
    #这里facts就是us-gapp里面的数据
    """Revenues（收入）
CostOfGoodsSold（成本）
GrossProfit（毛利）
OperatingIncomeLoss（营业利润）
NetIncomeLoss（净利润）
Assets（资产）
Liabilities（负债）
StockholdersEquity（股东权益）
CashAndCashEquivalents
OperatingCashFlow（经营现金流）
CapitalExpenditures（资本开支）
EPS（每股收益）
Segment 信息（有时）
Geographic 信息（极少）"""
    all_data = []
    
    # 遍历每个财务指标
    for metric_key, metric_name in FINANCIAL_METRICS.items():
        if metric_name not in facts:
            continue
        
        metric_data = facts[metric_name]
        units = metric_data.get("units", {})
        
        # 通常使用USD单位, units是次一级的列表
        if "USD" not in units:
            # 尝试其他单位
            available_units = list(units.keys())
            if available_units:
                unit_key = available_units[0]
            else:
                continue
        else:
            unit_key = "USD"
        
        for record in units[unit_key]:
            # record 就是一条具体的财务数据
            end_date = datetime.strptime(record["end"], "%Y-%m-%d")
            filed_date = datetime.strptime(record["filed"], "%Y-%m-%d")
            
            # 只保留过去5年的数据
            if filed_date >= cutoff_date:
                all_data.append({
                    "metric": metric_key,
                    "metric_name": metric_name,
                    "value": record["val"],
                    "unit": unit_key,
                    "fiscal_year": record.get("fy"),
                    "fiscal_period": record.get("fp"),  # Q1, Q2, Q3, Q4, FY
                    "form": record.get("form"),  # 10-K or 10-Q
                    "filed_date": record["filed"],
                    "end_date": record["end"],
                    "frame": record.get("frame"),  # 用于季度数据的年度标识
                    "Accession_Number" : record.get("accn")   #要不要记上这个
                })
    
    df = pd.DataFrame(all_data)
    return df
#返回us-gaap 的df

def standardize_financial_data(df_financial):  #我想这一步把10K 和 10Q区分开，并尝试在这里算 change in cash flow(compared to last year)
    """
    可能是用extract_financial_data 的 df ？
    标准化财务数据：统一单位、验证数据质量
    返回的df 按fiscal_year和fiscal_period排序
    Args:
        df_financial: extract_financial_data返回的DataFrame
    
    Returns:
        标准化后的DataFrame
    """
    # ！！！ 确保所有值都是USD，以下这一行使得单位为share 删除了，这不是我们想要的，先屏蔽喽
    #df_standardized = df_financial[df_financial["unit"] == "USD"].copy()
    df_standardized = df_financial
    
    # 按fiscal_year和fiscal_period排序
    df_standardized = df_standardized.sort_values(
        ["fiscal_year", "fiscal_period"], 
        ascending=[False, False]
    )

    return df_standardized

def check_cashflow_balance(df_financial, cik):
    """
    检查现金流平衡：OCF + ICF + FCF ≈ ΔCash
    
    使用年度数据（10-K文件），计算每年的现金流变化
    
    Args:
        df_financial: 标准化后的财务数据DataFrame
        cik: CIK用于错误报告
    
    Returns:
        cashflow_report: 现金流检查报告字典
    """
    cashflow_report = {
        "cik": cik,
        "checks": [],
        "warnings": [],
        "errors": [],
    }
    
    # 筛选年度数据（fiscal_period == "FY"）
    df_annual = df_financial[df_financial["fiscal_period"] == "FY"].copy()
    
    # 检查必要的指标是否存在
    required_metrics = ["ocf", "icf", "fcf", "capex", "cash"]
    available_metrics = df_annual["metric"].unique()
    missing_metrics = [m for m in required_metrics if m not in available_metrics]
    
    if missing_metrics:
        cashflow_report["errors"].append(
            f"Missing required cashflow metrics: {missing_metrics}"
        )
        return cashflow_report,{} #!!!!!!!!!!!!!!!!!!!!
    
    # 按fiscal_year排序（升序，以便计算年度差值）
    df_annual_sorted = df_annual.sort_values("fiscal_year", ascending=True)
    years = sorted(df_annual_sorted["fiscal_year"].unique()) #提取年份(eg. 2020, 2021, 2022....)
    
    # 存储每年的现金余额，用于计算ΔCash
    cash_by_year = {}
    
    # 第一遍：收集每年的OCF, ICF, FCF, Cash值
    for year in years:
        year_data = df_annual_sorted[df_annual_sorted["fiscal_year"] == year].copy()
        
        # 如果有多个记录，按filed_date取最新的
        year_data_sorted = year_data.sort_values("filed_date", ascending=False)
        
        # 获取各个指标的值
        ocf_data = year_data_sorted[year_data_sorted["metric"] == "ocf"]
        icf_data = year_data_sorted[year_data_sorted["metric"] == "icf"]
        fcf_data = year_data_sorted[year_data_sorted["metric"] == "fcf"]
        cash_data = year_data_sorted[year_data_sorted["metric"] == "cash"]
        capex_data = year_data_sorted[year_data_sorted["metric"] == "capex"]
        
        if len(ocf_data) > 0 and len(icf_data) > 0 and len(fcf_data) > 0 and len(cash_data) > 0:
            ocf_val = ocf_data.iloc[0]["value"]
            icf_val = icf_data.iloc[0]["value"]
            fcf_val = fcf_data.iloc[0]["value"]
            cash_val = cash_data.iloc[0]["value"]
            capex_val = capex_data.iloc[0]["value"]
            
            cash_by_year[year] = {
                "ocf": ocf_val,
                "icf": icf_val,
                "fcf": fcf_val,
                "cash": cash_val,
                "capex": capex_val
            } #洗完了每一年的数据： ocf, icf, fcf, cash, capex
    df_cash_by_year = pd.DataFrame(cash_by_year)
    # 第二遍：计算现金流平衡
    for i, year in enumerate(years):
        if year not in cash_by_year:
            continue
        # 提取每年现金流的数据
        year_data = cash_by_year[year]
        ocf = year_data["ocf"]
        icf = year_data["icf"]
        fcf = year_data["fcf"]
        capex = year_data["capex"]
        cash_current = year_data["cash"]
        
        # 计算现金流总和
        # cashflow_sum = ocf + icf + fcf
        cashflow_sum = ocf - abs(capex)
        
        # 计算ΔCash = Cash(t) - Cash(t-1)
        if i == 0:
            # 第一年没有上一年数据
            cashflow_report["warnings"].append(
                f"FY{year}: Cashflow balance check skipped - First year, no prior cash balance"
            )
            continue
        
        prev_year = years[i - 1]
        if prev_year not in cash_by_year:
            cashflow_report["warnings"].append(
                f"FY{year}: Cashflow balance check skipped - Previous year ({prev_year}) data missing"
            )
            continue
        
        cash_prev = cash_by_year[prev_year]["cash"]
        delta_cash = cash_current - cash_prev
        
        # 计算差异百分比
        # 使用绝对值较大的作为分母，避免除零或极小值问题
        denominator = max(abs(cashflow_sum), abs(fcf))
        if denominator == 0:
            # 如果两者都是0，认为是一致的，通过pass
            cashflow_report["checks"].append(
                f"FY{year}: Cashflow balance check passed (both values are zero)"
            )
            continue
        
        diff = abs(cashflow_sum - fcf)
        diff_pct = (diff / denominator) * 100
        
        # 检查是否超过阈值
        if diff_pct > cashflow_quality_threshold: #目前设定的是1%
            cashflow_report["warnings"].append(
                f"FY{year}: Cashflow balance check failed. "
                f"OCF+ICF+FCF: ${cashflow_sum:,.0f}, ΔCash: ${delta_cash:,.0f}, "
                f"Difference: ${diff:,.0f} ({diff_pct:.2f}%)"
            )
        else:
            cashflow_report["checks"].append(
                f"FY{year}: Cashflow balance check passed. "
                f"OCF+ICF+FCF: ${cashflow_sum:,.0f}, ΔCash: ${delta_cash:,.0f}, "
                f"Difference: ${diff:,.0f} ({diff_pct:.2f}%)"
            )
    
    return cashflow_report, df_cash_by_year

def quality_check_financial_data(df_financial, cik):
    """
    执行基本的数据质量检查
    1%的threshold；
    现金流计算还没完成,目前做的是按照1年的来，后面也可以改成按照一个季度的来算
    Args:
        df_financial: 标准化后的财务数据DataFrame
        cik: CIK用于错误报告
    其中这块会不会不太严谨，只选iloc[0]?
    
    Returns:
        quality_report: 质量检查报告字典
    """
    report = {
        "cik": cik,
        "checks": [],
        "warnings": [],
        "errors": [],
    }
    
    # 检查1: Assets ≈ Liabilities + Equity
    # 获取年度数据（fiscal_period == "FY"）
    df_annual = df_financial[df_financial["fiscal_period"] == "FY"].copy()  #这块要确定是FY、fy还是fiscal_year（哦哦，不用了）
    
    for year in df_annual["fiscal_year"].unique():
        year_data = df_annual[df_annual["fiscal_year"] == year]
        
        assets = year_data[year_data["metric"] == "assets"]["value"]  # dataframe of asset, liability, equity with their value
        liabilities = year_data[year_data["metric"] == "liabilities"]["value"]
        equity = year_data[year_data["metric"] == "equity"]["value"]
        
        if len(assets) > 0 and len(liabilities) > 0 and len(equity) > 0:  #这块会不会不太严谨，只选iloc[0]?
            assets_val = assets.iloc[0]
            liabilities_val = liabilities.iloc[0]
            equity_val = equity.iloc[0]
            balance = assets_val - (liabilities_val + equity_val)
            balance_pct = abs(balance / assets_val * 100) if assets_val != 0 else 0
            
            if balance_pct > asset_quality_threshold:  # 超过1%偏差(在文件开头调整)
                report["warnings"].append(
                    f"FY{year}: Assets balance check failed. "
                    f"Difference: ${balance:,.0f} ({balance_pct:.2f}%)"
                )
            else:
                report["checks"].append(
                    f"FY{year}: Assets balance check passed"
                )
    
    # 检查2: OCF + ICF + FCF ≈ ΔCash
    cashflow_report = check_cashflow_balance(df_financial, cik)
    report["checks"].extend(cashflow_report["checks"])
    report["warnings"].extend(cashflow_report["warnings"])
    report["errors"].extend(cashflow_report["errors"])
    
    return report

def get_financial_data_for_company(cik, years_back=5):
    """
    完整的财务数据获取流程
    
    Args:
        cik: 10位CIK字符串
        years_back: 回溯年数
    
    Returns:
        (df_financial, quality_report)
    """
    # 1. 获取companyfacts
    company_facts = fetch_company_facts(cik)
    
    # 2. 提取财务数据
    df_financial = extract_financial_data(company_facts, years_back)
    
    # 3. 标准化
    df_standardized = standardize_financial_data(df_financial)
    
    # 4. 质量检查
    quality_report = quality_check_financial_data(df_standardized, cik)

    cashflow_report, df_cash_by_year = check_cashflow_balance(df_standardized,cik)
    
    return df_standardized, quality_report, cashflow_report, df_cash_by_year

if __name__ == "__main__":
    # 测试：获取AAPL的财务数据
    cik_aapl = "0001045810"
    df_financial, report, cashreport, each_yearcash = get_financial_data_for_company(cik_aapl, years_back=5)
    #df_financial,_ = get_financial_data_for_company(cik_aapl, years_back=5)
    print(f"\nFinancial Data for CIK {cik_aapl}:")
    print(df_financial.head(20))
    print(cashreport.head(10))
    print(each_yearcash.head(10))
    #df_financial.to_csv("df_financial.csv")

    print(f"\nQuality Report:")
    print(f"Checks: {len(report['checks'])}")
    print(f"Warnings: {len(report['warnings'])}")
    for warning in report["warnings"]:
        print(f"  - {warning}")
