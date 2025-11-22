"""
ETL pipeline with Supabase + VNStock:
- Extract 3 báo cáo tài chính từ VNStock
- Transform (không lỗi)
- Load vào Supabase: income_statement, balance_sheet, cash_flow
- Upload file CSV lên bucket processed-data
"""

import pandas as pd
from sqlalchemy import create_engine
from supabase import create_client, Client
from vnstock import Vnstock

# ==== CONFIG: sửa 2 giá trị này ====
DB_PASSWORD = "Chien-1207"                     # mật khẩu DB
SUPABASE_SERVICE_KEY = "sb_secret_qzMFzF85u7PxwvJmTVHooQ_Q9Tj7Zf9"     # lấy tại Project Settings -> API
# ===================================

DB_USER = "postgres"
DB_HOST = "db.fxjrsxepzrbpmqygfvee.supabase.co"
DB_NAME = "postgres"
DB_PORT = 5432

SUPABASE_URL = "https://fxjrsxepzrbpmqygfvee.supabase.co"

def run_etl():
    print("🔹 Extract: dùng VNStock để lấy báo cáo tài chính FPT...")

    stock = Vnstock().stock(symbol='FPT', source='VCI')

    # 1) Income Statement (KQKD)
    income_df = stock.finance.income_statement(period='year', lang='vi', dropna=True)
    
    # 2) Balance Sheet (BCĐKT)
    balance_df = stock.finance.balance_sheet(period='year', lang='vi', dropna=True)
    
    # 3) Cash Flow (LCTT)
    cashflow_df = stock.finance.cash_flow(period='year', dropna=True)

    print("➡ Income Statement sample:")
    print(income_df.head())

    # ====== TRANSFORM ======
    print("🔹 Transform: chuẩn hóa dữ liệu ...")

    # Thêm cột ticker nếu thiếu
    for df in [income_df, balance_df, cashflow_df]:
        if "ticker" not in df.columns:
            df["ticker"] = "FPT"

    # Lưu file CSV
    income_df.to_csv("income_statement.csv", index=False)
    balance_df.to_csv("balance_sheet.csv", index=False)
    cashflow_df.to_csv("cash_flow.csv", index=False)

    print("Đã lưu 3 file CSV.")

    # ====== LOAD vào Supabase PostgreSQL ======
    print("🔹 Load: ghi dữ liệu vào Supabase ...")

    engine = create_engine(
        f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    income_df.to_sql("fpt_income_statement", con=engine, if_exists="replace", index=False)
    balance_df.to_sql("fpt_balance_sheet", con=engine, if_exists="replace", index=False)
    cashflow_df.to_sql("fpt_cash_flow", con=engine, if_exists="replace", index=False)

    print("Đã ghi 3 bảng vào Supabase PostgreSQL")

    # ====== UPLOAD STORAGE ======
    print("🔹 Upload 3 file CSV lên bucket processed-data ...")

    supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

    files = [
        ("income_statement.csv", "income_statement.csv"),
        ("balance_sheet.csv", "balance_sheet.csv"),
        ("cash_flow.csv", "cash_flow.csv"),
    ]

    for local, remote in files:
        with open(local, "rb") as f:
            res = supabase.storage.from_("processed-data").upload(remote, f)
            print(f"Uploaded {local}:", res)

    print("✅ ETL hoàn tất!")

if __name__ == "__main__":
    run_etl()
