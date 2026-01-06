# from first_01 import df
# #print(df)
#
# import requests
# import json
# import pandas as pd
#
#
# def fetch_and_convert_tickers(url="https://data.sec.gov/submissions/CIK0002093907.json"):
#     # 加 SEC 要求的 User-Agent
#     headers = {
#         "User-Agent": "Kevin-Research (kevinbotaosong@ucla.edu)"
#     }
#
#     # 请求 JSON
#     resp = requests.get(url, headers=headers)
#     resp.raise_for_status()
#     data = resp.json()
