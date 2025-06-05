from airflow.hooks.base import BaseHook
import json
import requests
from minio import Minio
from io import BytesIO
from datetime import datetime
from airflow.exceptions import AirflowNotFoundException

bucket_name =  'stock-market'

def _get_minio_client():
    conn = BaseHook.get_connection("minio")
    client = Minio(
        endpoint=conn.extra_dejson['endpoint_url'].split('//')[1],
        access_key=conn.extra_dejson.get("aws_access_key_id", conn.login),
        secret_key=conn.extra_dejson.get("aws_secret_access_key", conn.password),
        secure=False
    )
    return client


def _get_stock_prices(url, symbol):
    try:
        full_url = f"{url}{symbol}?interval=1d&range=1y&includePrePost=false"
        conn = BaseHook.get_connection("yahoo_finance_conn")
        headers = conn.extra_dejson.get("headers", {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json"
        })
        
        response = requests.get(full_url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        chart_data = data['chart']['result'][0]

        return json.dumps({
            "meta": chart_data.get("meta", {}),
            "timestamp": chart_data.get("timestamp", []),
            "indicators": chart_data.get("indicators", {})
        })
    except Exception as e:
        raise ValueError(f"获取股票数据失败: {str(e)}")


def _store_prices(stock, execution_date=None):
    minio = BaseHook.get_connection("minio")
    client = _get_minio_client()


    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    stock = json.loads(stock)
    symbol = stock['meta']['symbol']
    data = json.dumps(stock, ensure_ascii=False).encode('utf-8')

    # ➕ 使用 execution_date 作为文件名的一部分
    if not execution_date:
        execution_date = datetime.utcnow().strftime("%Y-%m-%d")
    else:
        execution_date = datetime.fromisoformat(execution_date).strftime("%Y-%m-%d")

    object_name = f"{symbol}/{execution_date}/prices.json"

    client.put_object(
        bucket_name=bucket_name,
        object_name=object_name,
        data=BytesIO(data),
        length=len(data),
        content_type="application/json"
    )

    return f'{bucket_name}/{symbol}/{execution_date}'

def _get_formatted_csv(path):
    client = _get_minio_client()
    path_parts = path.split('/')
    if len(path_parts) < 3:
        raise ValueError("路径格式不正确，应包含至少三个部分：bucket/symbol/date")

    symbol = path_parts[1]
    date_str = path_parts[2]
    prefix_name = f"{symbol}/{date_str}/formatted_prices/"

    objects = client.list_objects(bucket_name, prefix=prefix_name, recursive=True)
    for obj in objects:
        if obj.object_name.endswith('.csv'):
            return obj.object_name

    raise AirflowNotFoundException('This CSV file does not exist')

