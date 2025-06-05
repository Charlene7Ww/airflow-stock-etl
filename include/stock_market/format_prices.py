# include/stock_market/format_prices.py
import os
import json
import pandas as pd
from minio import Minio
from io import BytesIO

def main():
<<<<<<< HEAD
    base_path = os.environ.get("SPARK_APPLICATION_ARGS")  # 形如 stock-market/NVDA/2025-05-27
=======
    base_path = os.environ.get("SPARK_APPLICATION_ARGS")  # stock-market/NVDA/2025-05-27
>>>>>>> 82de2d6 (Update)
    if not base_path:
        raise ValueError("Missing SPARK_APPLICATION_ARGS")

    bucket = "stock-market"
    endpoint = os.environ.get("MINIO_ENDPOINT", "external-minio:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "minio")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "minio123")

    client = Minio(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False
    )

    parts = base_path.strip("/").split('/')
    if len(parts) < 3:
        raise ValueError(f"Invalid path: {base_path}")
    symbol, date = parts[1], parts[2]
    object_name = f"{symbol}/{date}/prices.json"

    print(f"📥 Downloading from MinIO: {object_name}")
    response = client.get_object(bucket, object_name)
    raw_data = json.loads(response.read())

    timestamps = raw_data["timestamp"]
    quote = raw_data["indicators"]["quote"][0]

    df = pd.DataFrame({
        "symbol": [symbol] * len(timestamps),
        "date": pd.to_datetime(timestamps, unit='s'),
        "open": quote["open"],
        "high": quote["high"],
        "low": quote["low"],
        "close": quote["close"],
        "volume": quote["volume"]
    })

    csv_data = df.to_csv(index=False).encode("utf-8")
    csv_object_name = f"{symbol}/{date}/formatted_prices/formatted.csv"

    print(f"📤 Uploading to MinIO: {csv_object_name}")
    client.put_object(
        bucket,
        csv_object_name,
        data=BytesIO(csv_data),
        length=len(csv_data),
        content_type="text/csv"
    )
<<<<<<< HEAD
    print("✅ CSV uploaded successfully!")
=======
    print("CSV uploaded successfully")
>>>>>>> 82de2d6 (Update)

if __name__ == "__main__":
    main()

