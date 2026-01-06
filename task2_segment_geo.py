import requests
import json
import pandas as pd
from bs4 import BeautifulSoup
import re
from datetime import datetime
from first_01 import get_sec_headers
from task1_filings import get_filings_for_company
#新建 task2_segment_geo.py - 抓取segment和geographic数据

def fetch_10k_html(filing_url):
    """获取10-K HTML内容"""
    resp = requests.get(filing_url, headers=get_sec_headers())
    resp.raise_for_status()
    return resp.text

def find_segment_table(html_content):
    """
    在10-K HTML中查找Segment Information表格
    
    Returns:
        (table_element, section_name) 或 (None, None)
    """
    soup = BeautifulSoup(html_content, "html.parser")
    
    # 查找包含"Segment"关键词的section
    segment_keywords = [
        "segment information",
        "operating segments",
        "reportable segments",
    ]
    
    # 方法1: 查找标题
    for keyword in segment_keywords:
        # 查找包含关键词的标题（h1-h6, p, div等）
        headers = soup.find_all(
            ["h1", "h2", "h3", "h4", "h5", "h6", "p", "div"],
            string=re.compile(keyword, re.I)
        )
        
        for header in headers:
            # 在header附近查找table
            parent = header.find_parent()
            if parent:
                table = parent.find("table")
                if table:
                    return table, keyword
    
    # 方法2: 直接查找包含"segment"的table
    tables = soup.find_all("table")
    for table in tables:
        table_text = table.get_text().lower()
        if any(keyword in table_text for keyword in segment_keywords):
            return table, "segment_table"
    
    return None, None

def find_geographic_table(html_content):
    """
    在10-K HTML中查找Geographic Information表格
    
    Returns:
        (table_element, section_name) 或 (None, None)
    """
    soup = BeautifulSoup(html_content, "html.parser")
    
    geo_keywords = [
        "geographic information",
        "geographic revenue",
        "revenue by geographic",
        "revenue by region",
    ]
    
    # 方法1: 查找标题
    for keyword in geo_keywords:
        headers = soup.find_all(
            ["h1", "h2", "h3", "h4", "h5", "h6", "p", "div"],
            string=re.compile(keyword, re.I)
        )
        
        for header in headers:
            parent = header.find_parent()
            if parent:
                table = parent.find("table")
                if table:
                    return table, keyword
    
    # 方法2: 直接查找包含"geographic"的table
    tables = soup.find_all("table")
    for table in tables:
        table_text = table.get_text().lower()
        if any(keyword in table_text for keyword in geo_keywords):
            return table, "geographic_table"
    
    return None, None

def parse_revenue_table(table, table_type="segment"):
    """
    解析revenue表格，提取逐年数据
    
    Args:
        table: BeautifulSoup table元素
        table_type: "segment" 或 "geographic"
    
    Returns:
        DataFrame包含逐年revenue数据
    """
    # 提取所有行
    rows = table.find_all("tr")
    if len(rows) < 2:
        return pd.DataFrame()
    
    # 尝试找到表头（包含年份）
    header_row = None
    for i, row in enumerate(rows[:5]):  # 检查前5行
        cells = row.find_all(["th", "td"])
        cell_texts = [cell.get_text(strip=True) for cell in cells]
        # 检查是否包含年份（4位数字）
        if any(re.match(r"^\d{4}$", text) for text in cell_texts):
            header_row = i
            break
    
    if header_row is None:
        return pd.DataFrame()
    
    # 提取年份列
    header_cells = rows[header_row].find_all(["th", "td"])
    years = []
    year_indices = []
    
    for idx, cell in enumerate(header_cells):
        text = cell.get_text(strip=True)
        if re.match(r"^\d{4}$", text):
            years.append(int(text))
            year_indices.append(idx)
    
    if len(years) == 0:
        return pd.DataFrame()
    
    # 提取数据行
    data_rows = []
    for row in rows[header_row + 1:]:
        cells = row.find_all(["td", "th"])
        if len(cells) < max(year_indices) + 1:
            continue
        
        # 第一列通常是segment/region名称
        name = cells[0].get_text(strip=True)
        if not name or len(name) < 2:
            continue
        
        # 提取各年份的数值
        row_data = {"name": name}
        for year, idx in zip(years, year_indices):
            if idx < len(cells):
                value_text = cells[idx].get_text(strip=True)
                # 清理数值（去除$、逗号等）
                value_text = re.sub(r"[$,()]", "", value_text)
                # 处理负数（括号表示）
                if "(" in cells[idx].get_text() or value_text.startswith("-"):
                    multiplier = -1
                else:
                    multiplier = 1
                
                try:
                    # 尝试提取数字（可能是百万或千）
                    numbers = re.findall(r"[\d.]+", value_text)
                    if numbers:
                        value = float(numbers[0]) * multiplier
                        # 检查单位（通常表格标题会说明是millions还是thousands）
                        # 这里假设是millions，需要根据实际情况调整
                        row_data[f"year_{year}"] = value
                except:
                    pass
        
        if len([k for k in row_data.keys() if k.startswith("year_")]) > 0:
            data_rows.append(row_data)
    
    df = pd.DataFrame(data_rows)
    return df

def get_segment_geographic_data(cik, years_back=5):
    """
    获取公司的segment和geographic revenue数据
    
    Args:
        cik: 10位CIK字符串
        years_back: 回溯年数
    
    Returns:
        (df_segment, df_geographic, missing_reports)
    """
    # 1. 获取所有10-K文件
    df_filings = get_filings_for_company(cik, years_back)
    df_10k = df_filings[df_filings["form"].isin(["10-K", "10-K/A"])].copy()
    
    all_segment_data = []
    all_geo_data = []
    missing_reports = []
    
    for _, filing in df_10k.iterrows():
        filing_url = filing["filing_url"]
        filing_date = filing["filing_date"]
        
        try:
            # 获取HTML
            html_content = fetch_10k_html(filing_url)
            
            # 查找Segment表格
            segment_table, segment_section = find_segment_table(html_content)
            if segment_table:
                df_segment = parse_revenue_table(segment_table, "segment")
                if len(df_segment) > 0:
                    df_segment["filing_date"] = filing_date
                    df_segment["filing_url"] = filing_url
                    df_segment["source_section"] = segment_section
                    all_segment_data.append(df_segment)
            else:
                missing_reports.append({
                    "filing_date": filing_date,
                    "type": "segment",
                    "url": filing_url,
                })
            
            # 查找Geographic表格
            geo_table, geo_section = find_geographic_table(html_content)
            if geo_table:
                df_geo = parse_revenue_table(geo_table, "geographic")
                if len(df_geo) > 0:
                    df_geo["filing_date"] = filing_date
                    df_geo["filing_url"] = filing_url
                    df_geo["source_section"] = geo_section
                    all_geo_data.append(df_geo)
            else:
                missing_reports.append({
                    "filing_date": filing_date,
                    "type": "geographic",
                    "url": filing_url,
                })
        
        except Exception as e:
            missing_reports.append({
                "filing_date": filing_date,
                "type": "error",
                "url": filing_url,
                "error": str(e),
            })
    
    # 合并所有数据
    df_segment_final = pd.concat(all_segment_data, ignore_index=True) if all_segment_data else pd.DataFrame()
    df_geo_final = pd.concat(all_geo_data, ignore_index=True) if all_geo_data else pd.DataFrame()
    
    return df_segment_final, df_geo_final, missing_reports

def validate_segment_geo_data(df_segment, df_geo, df_financial):
    """
    验证segment和geographic数据质量
    
    检查：segment/geo sum必须接近total revenue（≤3%偏差）
    """
    validation_report = {
        "segment_checks": [],
        "geo_checks": [],
        "warnings": [],
    }
    
    # 获取每年的total revenue
    df_annual_revenue = df_financial[
        (df_financial["metric"] == "revenue") & 
        (df_financial["fiscal_period"] == "FY")
    ].copy()
    
    # 验证segment数据
    if len(df_segment) > 0:
        for filing_date in df_segment["filing_date"].unique():
            segment_year = df_segment[df_segment["filing_date"] == filing_date]
            # 计算各年份的segment sum
            year_cols = [col for col in segment_year.columns if col.startswith("year_")]
            for col in year_cols:
                year = int(col.replace("year_", ""))
                segment_sum = segment_year[col].sum()
                
                # 找到对应的total revenue
                revenue_row = df_annual_revenue[df_annual_revenue["fiscal_year"] == year]
                if len(revenue_row) > 0:
                    total_revenue = revenue_row["value"].iloc[0]
                    if total_revenue > 0:
                        diff_pct = abs(segment_sum - total_revenue) / total_revenue * 100
                        if diff_pct > 3:
                            validation_report["warnings"].append(
                                f"FY{year}: Segment sum differs from total revenue by {diff_pct:.2f}%"
                            )
                        else:
                            validation_report["segment_checks"].append(
                                f"FY{year}: Segment validation passed ({diff_pct:.2f}% diff)"
                            )
    
    # 类似地验证geographic数据
    if len(df_geo) > 0:
        for filing_date in df_geo["filing_date"].unique():
            geo_year = df_geo[df_geo["filing_date"] == filing_date]
            year_cols = [col for col in geo_year.columns if col.startswith("year_")]
            for col in year_cols:
                year = int(col.replace("year_", ""))
                geo_sum = geo_year[col].sum()
                
                revenue_row = df_annual_revenue[df_annual_revenue["fiscal_year"] == year]
                if len(revenue_row) > 0:
                    total_revenue = revenue_row["value"].iloc[0]
                    if total_revenue > 0:
                        diff_pct = abs(geo_sum - total_revenue) / total_revenue * 100
                        if diff_pct > 3:
                            validation_report["warnings"].append(
                                f"FY{year}: Geographic sum differs from total revenue by {diff_pct:.2f}%"
                            )
                        else:
                            validation_report["geo_checks"].append(
                                f"FY{year}: Geographic validation passed ({diff_pct:.2f}% diff)"
                            )
    
    return validation_report

if __name__ == "__main__":
    # 测试：获取AAPL的segment和geographic数据
    cik_aapl = "0000320193"
    df_segment, df_geo, missing = get_segment_geographic_data(cik_aapl, years_back=5)
    
    print(f"\nSegment Data:")
    print(df_segment)
    
    print(f"\nGeographic Data:")
    print(df_geo)
    
    print(f"\nMissing Reports:")
    for m in missing:
        print(f"  - {m['filing_date']}: {m['type']}")
