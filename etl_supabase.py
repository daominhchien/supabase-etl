"""
ETL pipeline with Supabase + VNStock:
- Extract 3 báo cáo tài chính từ VNStock
- Transform
- Load vào Supabase PostgreSQL:
    + fpt_income_statement
    + fpt_balance_sheet
    + fpt_cash_flow
- Upload file CSV lên bucket processed-data
"""

import os
import pandas as pd
from sqlalchemy import create_engine
from supabase import create_client, Client
from vnstock import Vnstock

# ==== CONFIG: có thể lấy từ ENV hoặc dùng default ====
DB_PASSWORD = os.getenv("DB_PASSWORD", "Chien-1207")  # mật khẩu DB Supabase

SUPABASE_SERVICE_KEY = os.getenv(
    "SUPABASE_SERVICE_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZ4anJzeGVwenJicG1xeWdmdmVlIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2Mzc0NjQxMSwiZXhwIjoyMDc5MzIyNDExfQ.qgE3kuh3ntg0t_YZxoJ5dHWS6Y9eWGeJrl_miJVucQs"
)
# ===================================

DB_USER = "postgres"
DB_HOST = "db.fxjrsxepzrbpmqygfvee.supabase.co"
DB_NAME = "postgres"
DB_PORT = 5432

SUPABASE_URL = "https://fxjrsxepzrbpmqygfvee.supabase.co"


def run_etl():
    # 1) EXTRACT
    print("🔹 Extract: dùng VNStock để lấy báo cáo tài chính FPT...")

    stock = Vnstock().stock(symbol="FPT", source="VCI")

    # 1) Income Statement (KQKD)
    income_df = stock.finance.income_statement(period="year", lang="vi", dropna=True)

    # 2) Balance Sheet (BCĐKT)
    balance_df = stock.finance.balance_sheet(period="year", lang="vi", dropna=True)

    # 3) Cash Flow (LCTT)
    cashflow_df = stock.finance.cash_flow(period="year", dropna=True)

    print("➡ Income Statement sample:")
    print(income_df.head())

    # 2) TRANSFORM
    print("🔹 Transform: chuẩn hóa dữ liệu ...")

    # Thêm cột ticker nếu thiếu
    for df in (income_df, balance_df, cashflow_df):
        if "ticker" not in df.columns:
            df["ticker"] = "FPT"

    # Lưu file CSV
    income_df.to_csv("income_statement.csv", index=False)
    balance_df.to_csv("balance_sheet.csv", index=False)
    cashflow_df.to_csv("cash_flow.csv", index=False)
    print("✅ Đã lưu 3 file CSV.")

    # 3) LOAD → PostgreSQL Supabase
    print("🔹 Load: ghi dữ liệu vào Supabase PostgreSQL ...")

    engine = create_engine(
        f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    # Ghi đè bảng mỗi lần chạy
    income_df.to_sql(
        "fpt_income_statement", con=engine, if_exists="replace", index=False
    )
    balance_df.to_sql(
        "fpt_balance_sheet", con=engine, if_exists="replace", index=False
    )
    cashflow_df.to_sql(
        "fpt_cash_flow", con=engine, if_exists="replace", index=False
    )

    print("✅ Đã ghi 3 bảng vào Supabase PostgreSQL.")

    # 4) UPLOAD CSV → STORAGE
    print("🔹 Upload 3 file CSV lên bucket processed-data ...")

    supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    bucket = supabase.storage.from_("processed-data")

    files = [
        ("income_statement.csv", "income_statement.csv"),
        ("balance_sheet.csv", "balance_sheet.csv"),
        ("cash_flow.csv", "cash_flow.csv"),
    ]

    for local, remote in files:
        with open(local, "rb") as f:
            try:
                # nếu file tồn tại thì update, nếu không thì upload
                res = bucket.update(remote, f)
            except Exception:
                f.seek(0)
                res = bucket.upload(remote, f)
            print(f"Uploaded {local} -> {remote}: {res}")

    print("✅ ETL hoàn tất!")


if __name__ == "__main__":
    run_etl()
