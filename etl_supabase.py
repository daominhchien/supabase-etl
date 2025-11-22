"""
ETL pipeline with Supabase + VNStock (REST API version for GitHub Actions)
"""

import os
import json
import pandas as pd
from vnstock import Vnstock
from supabase import create_client, Client
from unidecode import unidecode

# ====== CONFIG ======
SUPABASE_URL = "https://fxjrsxepzrbpmqygfvee.supabase.co"

SUPABASE_SERVICE_KEY = os.getenv(
    "SUPABASE_SERVICE_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZ4anJzeGVwenJicG1xeWdmdmVlIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2Mzc0NjQxMSwiZXhwIjoyMDc5MzIyNDExfQ.qgE3kuh3ntg0t_YZxoJ5dHWS6Y9eWGeJrl_miJVucQs"
)

def df_to_utf8_dict(df):
    return json.loads(df.to_json(orient="records", force_ascii=False))

# === Chuẩn hóa tên cột: bỏ dấu + bỏ ngoặc + bỏ space ===
def normalize_columns(df):
    new_cols = {}
    for c in df.columns:
        name = unidecode(c)                 # bỏ dấu
        name = name.replace(" ", "_")       # đổi space -> _
        name = name.replace("(", "").replace(")", "")  # bỏ ngoặc
        name = name.replace("/", "_")
        name = name.lower().strip()         # lower
        new_cols[c] = name
    return df.rename(columns=new_cols)

# ====== HÀM CHÍNH ======
def run_etl():
    print("🔹 Extract: dùng VNStock để lấy báo cáo tài chính FPT...")

    stock = Vnstock().stock(symbol="FPT", source="VCI")

    income_df = stock.finance.income_statement(period="year", lang="vi", dropna=True)
    balance_df = stock.finance.balance_sheet(period="year", lang="vi", dropna=True)
    cashflow_df = stock.finance.cash_flow(period="year", dropna=True)

    print("➡ Income Statement sample:")
    print(income_df.head())

    print("🔹 Transform: normalize column names ...")

    income_df = normalize_columns(income_df)
    balance_df = normalize_columns(balance_df)
    cashflow_df = normalize_columns(cashflow_df)

    for df in (income_df, balance_df, cashflow_df):
        if "ticker" not in df.columns:
            df["ticker"] = "FPT"

    income_df.to_csv("income_statement.csv", index=False)
    balance_df.to_csv("balance_sheet.csv", index=False)
    cashflow_df.to_csv("cash_flow.csv", index=False)
    print("Đã lưu 3 file CSV.")

    print("🔹 Load: upsert dữ liệu vào Supabase qua REST API ...")

    supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

    income_data = df_to_utf8_dict(income_df)
    balance_data = df_to_utf8_dict(balance_df)
    cashflow_data = df_to_utf8_dict(cashflow_df)

    resp1 = supabase.table("fpt_income_statement").upsert(income_data).execute()
    print("Upsert fpt_income_statement:", resp1)

    resp2 = supabase.table("fpt_balance_sheet").upsert(balance_data).execute()
    print("Upsert fpt_balance_sheet:", resp2)

    resp3 = supabase.table("fpt_cash_flow").upsert(cashflow_data).execute()
    print("Upsert fpt_cash_flow:", resp3)

    print("🔹 Upload 3 file CSV lên bucket processed-data ...")

    files = [
        ("income_statement.csv", "income_statement.csv"),
        ("balance_sheet.csv", "balance_sheet.csv"),
        ("cash_flow.csv", "cash_flow.csv"),
    ]

    bucket = supabase.storage.from_("processed-data")

    for local, remote in files:
        with open(local, "rb") as f:
            try:
                res = bucket.update(remote, f)
            except Exception:
                res = bucket.upload(remote, f)
            print(f"Uploaded {local}:", res)

    print("✅ ETL hoàn tất!")

if __name__ == "__main__":
    run_etl()
