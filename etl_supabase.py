"""
ETL pipeline with Supabase REST API + VNStock:
- Extract 3 báo cáo tài chính từ VNStock
- Transform: pack dữ liệu vào cột data (JSONB)
- Load vào Supabase PostgreSQL qua REST API
"""

import os
import json
import pandas as pd
import requests
from vnstock import Vnstock

SUPABASE_URL = os.getenv("SUPABASE_URL", "https://tzwepclhllftfmoeimjd.supabase.co")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

if not SUPABASE_SERVICE_KEY:
    raise RuntimeError("Thiếu SUPABASE_SERVICE_KEY trong ENV")

REST_BASE_URL = f"{SUPABASE_URL}/rest/v1"
STORAGE_BASE_URL = f"{SUPABASE_URL}/storage/v1"

COMMON_HEADERS = {
    "apikey": SUPABASE_SERVICE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
}

JSON_HEADERS = {
    **COMMON_HEADERS,
    "Content-Type": "application/json",
    "Prefer": "return=minimal"
}


def df_to_jsonb_records(df: pd.DataFrame):
    """
    Convert DataFrame to records with JSONB format.
    Giả sử DataFrame có cột 'Năm' hoặc 'Year', các cột khác pack vào 'data'.
    """
    records = []
    
    # Tìm cột năm (Năm hoặc Year)
    year_col = None
    for col in df.columns:
        if col.lower() in ['năm', 'year']:
            year_col = col
            break
    
    for _, row in df.iterrows():
        year = int(row[year_col]) if year_col and pd.notna(row[year_col]) else None
        
        # Pack toàn bộ dữ liệu vào JSONB
        data_dict = {}
        for col in df.columns:
            if col.lower() not in ['năm', 'year', 'cp', 'ticker']:
                val = row[col]
                data_dict[col] = None if pd.isna(val) else val
        
        ticker = row.get('CP') or row.get('ticker', 'FPT')
        
        record = {
            "ticker": ticker,
            "year": year,
            "data": data_dict
        }
        records.append(record)
    
    return records


def upsert_table(records: list, table_name: str, chunk_size: int = 300):
    """Gửi dữ liệu lên Supabase REST API theo từng chunk."""
    print(f"🔹 Upsert {len(records)} rows vào bảng {table_name} qua REST API...")

    url = f"{REST_BASE_URL}/{table_name}"

    for i in range(0, len(records), chunk_size):
        chunk = records[i:i + chunk_size]
        resp = requests.post(url, headers=JSON_HEADERS, data=json.dumps(chunk))
        if not resp.ok:
            print(f"❌ Lỗi khi upsert chunk {i}-{i+len(chunk)} vào {table_name}: {resp.status_code}")
            print(resp.text)
            resp.raise_for_status()
        else:
            print(f"✅ Đã upsert {len(chunk)} rows vào {table_name}")


def upload_to_storage(local_path: str, remote_path: str, bucket: str = "processed-data"):
    """Upload file lên Supabase Storage qua REST API."""
    url = f"{STORAGE_BASE_URL}/object/{bucket}/{remote_path}"
    params = {"upsert": "true"}

    ext = os.path.splitext(local_path)[1].lower()
    content_type = "text/csv" if ext == ".csv" else "application/octet-stream"

    headers = {
        **COMMON_HEADERS,
        "Content-Type": content_type,
    }

    with open(local_path, "rb") as f:
        resp = requests.post(url, headers=headers, params=params, data=f)
        if not resp.ok:
            print(f"❌ Lỗi upload {local_path} -> {bucket}/{remote_path}: {resp.status_code}")
            print(resp.text)
            resp.raise_for_status()
        else:
            print(f"✅ Uploaded {local_path} -> {bucket}/{remote_path}")


def run_etl():
    # 1) EXTRACT
    print("🔹 Extract: dùng VNStock để lấy báo cáo tài chính FPT...")
    
    stock = Vnstock().stock(symbol="FPT", source="VCI")
    
    income_df = stock.finance.income_statement(period="year", lang="vi", dropna=True)
    balance_df = stock.finance.balance_sheet(period="year", lang="vi", dropna=True)
    cashflow_df = stock.finance.cash_flow(period="year", dropna=True)

    print("➡ Income Statement sample:")
    print(income_df.head())
    print(f"Columns: {income_df.columns.tolist()}")

    # 2) TRANSFORM
    print("🔹 Transform: chuẩn hóa dữ liệu...")
    
    income_records = df_to_jsonb_records(income_df)
    balance_records = df_to_jsonb_records(balance_df)
    cashflow_records = df_to_jsonb_records(cashflow_df)
    
    print(f"✅ Converted {len(income_records)} income records")
    print(f"✅ Converted {len(balance_records)} balance records")
    print(f"✅ Converted {len(cashflow_records)} cashflow records")
    
    print(f"\n📋 Sample income record: {json.dumps(income_records[0], ensure_ascii=False, indent=2)}")

    # Lưu CSV (original format)
    income_df.to_csv("income_statement.csv", index=False)
    balance_df.to_csv("balance_sheet.csv", index=False)
    cashflow_df.to_csv("cash_flow.csv", index=False)
    print("✅ Đã lưu 3 file CSV.")

    # 3) LOAD → Supabase qua REST API
    upsert_table(income_records, "fpt_income_statement")
    upsert_table(balance_records, "fpt_balance_sheet")
    upsert_table(cashflow_records, "fpt_cash_flow")

    print("✅ Đã gửi dữ liệu lên 3 bảng qua REST API.")

    # 4) UPLOAD CSV → STORAGE
    print("🔹 Upload 3 file CSV lên bucket processed-data...")

    upload_to_storage("income_statement.csv", "income_statement.csv")
    upload_to_storage("balance_sheet.csv", "balance_sheet.csv")
    upload_to_storage("cash_flow.csv", "cash_flow.csv")

    print("✅ ETL hoàn tất!")


if __name__ == "__main__":
    run_etl()

