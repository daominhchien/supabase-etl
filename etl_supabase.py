"""
ETL pipeline with Supabase + VNStock (REST API version for GitHub Actions)

- Extract: 3 báo cáo tài chính FPT từ VNStock
- Transform: chuẩn hóa & lưu CSV
- Load: upsert vào Supabase bằng REST API (supabase.table)
- Upload: đẩy CSV lên Supabase Storage (bucket: processed-data)
"""

import os
import json          # 👈 THÊM DÒNG NÀY
import pandas as pd
from vnstock import Vnstock
from supabase import create_client, Client

# ====== CONFIG ======

# URL project Supabase
SUPABASE_URL = "https://fxjrsxepzrbpmqygfvee.supabase.co"

# Lấy SERVICE KEY từ biến môi trường nếu có (GitHub Actions),
# nếu không thì fallback về giá trị bạn hard-code cho chạy local.
SUPABASE_SERVICE_KEY = os.getenv(
    "SUPABASE_SERVICE_KEY",
    "sb_secret_xxx_thay_bang_service_role_key_cua_ban"
)

# 👇 HÀM MỚI: convert DataFrame -> list[dict] UTF-8 safe
def df_to_utf8_dict(df):
    # dùng to_json(force_ascii=False) rồi parse lại thành Python object
    return json.loads(df.to_json(orient="records", force_ascii=False))

# ====== HÀM CHÍNH ======
def run_etl():
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

    # ====== TRANSFORM ======
    print("🔹 Transform: chuẩn hóa dữ liệu ...")

    # Thêm cột ticker nếu thiếu
    for df in (income_df, balance_df, cashflow_df):
        if "ticker" not in df.columns:
            df["ticker"] = "FPT"

    # Lưu 3 file CSV
    income_df.to_csv("income_statement.csv", index=False)
    balance_df.to_csv("balance_sheet.csv", index=False)
    cashflow_df.to_csv("cash_flow.csv", index=False)
    print("Đã lưu 3 file CSV.")

    # ====== LOAD: Supabase REST API qua supabase-py ======
    print("🔹 Load: upsert dữ liệu vào Supabase qua REST API ...")

    supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

    # ❌ ĐỪNG DÙNG .to_dict() nữa
    # income_data = income_df.to_dict(orient="records")
    # ✅ THAY BẰNG:
    income_data = df_to_utf8_dict(income_df)
    balance_data = df_to_utf8_dict(balance_df)
    cashflow_data = df_to_utf8_dict(cashflow_df)

    # Lưu ý:
    # - Bảng trong Supabase phải tồn tại sẵn:
    #   fpt_income_statement, fpt_balance_sheet, fpt_cash_flow
    # - Nên tạo PRIMARY KEY hoặc UNIQUE để upsert hoạt động đúng.
    resp1 = supabase.table("fpt_income_statement").upsert(income_data).execute()
    print("Upsert fpt_income_statement:", resp1)

    resp2 = supabase.table("fpt_balance_sheet").upsert(balance_data).execute()
    print("Upsert fpt_balance_sheet:", resp2)

    resp3 = supabase.table("fpt_cash_flow").upsert(cashflow_data).execute()
    print("Upsert fpt_cash_flow:", resp3)

    # ====== UPLOAD CSV LÊN STORAGE ======
    print("🔹 Upload 3 file CSV lên bucket processed-data ...")

    files = [
        ("income_statement.csv", "income_statement.csv"),
        ("balance_sheet.csv", "balance_sheet.csv"),
        ("cash_flow.csv", "cash_flow.csv"),
    ]

    for local, remote in files:
        with open(local, "rb") as f:
            # nếu file đã tồn tại thì dùng update, nếu lỗi thì fallback upload
            try:
                res = supabase.storage.from_("processed-data").update(remote, f)
            except Exception:
                res = supabase.storage.from_("processed-data").upload(remote, f)
            print(f"Uploaded {local}:", res)

    print("✅ ETL hoàn tất!")

# ====== ENTRYPOINT ======
if __name__ == "__main__":
    run_etl()
