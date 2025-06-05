from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from astro import sql as sql
from astro.files import File
from astro.sql.table import Table, Metadata
from datetime import datetime
from include.stock_market.tasks import _get_stock_prices, _store_prices, _get_formatted_csv
import requests

symbols = "NVDA"
bucket_name = "stock-market"

@dag(
    dag_id="stock_market_dag",
    start_date = datetime(2023, 1, 1),
    schedule = "@daily",
    catchup = False,
    tags = ['stock_market']) #taskflow api

def stock_market_dag(): # is_api_available_task
    @task.sensor(task_id="is_api_available",poke_interval =30, timeout = 300, mode="poke") # define how often to check API
    def is_api_available()-> PokeReturnValue:
        
        api = BaseHook.get_connection("yahoo_finance_conn")
        extra = api.extra_dejson
        url = f"{api.host}{extra['endpoint']}{symbols}?interval=1d" # 改了
        print(f"Checking API availability at {url}")
        response = requests.get(
            url,
            headers=extra["headers"],
            timeout=10
        ) # 改了
        response.raise_for_status()
        # condition = response.json()['finance']['result'] is not None
        condition = 'chart' in response.json()
        return PokeReturnValue(is_done = condition,
                               xcom_value=f"{api.host}{extra['endpoint']}") # xcom_value is the value to be passed to the next task ，改了

    get_stock_prices = PythonOperator(
        task_id = "get_stock_prices",
        python_callable = _get_stock_prices,
        op_kwargs = {'url':'{{ti.xcom_pull(task_ids = "is_api_available")}}', 
                     "symbol": symbols},
    ) # mixing decorator and docker operator is weird
    
    store_prices = PythonOperator(
        task_id = "store_prices",
        python_callable = _store_prices,
        op_kwargs = {
            'stock': '{{ti.xcom_pull(task_ids = "get_stock_prices")}}',
            'execution_date': '{{ ts }}'
        },
    )

    format_prices = DockerOperator(
        task_id="format_prices",
        image="new-udemy-project_6e227e/airflow:latest",
        command="python /usr/local/airflow/include/stock_market/format_prices.py",
        api_version='auto',
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="new-udemy-project_6e227e_airflow",  # ✅ 正确网络名
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        environment={
            'SPARK_APPLICATION_ARGS': '{{ti.xcom_pull(task_ids = "store_prices")}}',
            'MINIO_ENDPOINT': 'external-minio:9000', 
            'MINIO_ACCESS_KEY': 'minio',
            'MINIO_SECRET_KEY': 'minio123'
        }
    )


    get_formatted_csv = PythonOperator(
        task_id = "get_formatted_csv",
        python_callable = _get_formatted_csv,
        op_kwargs = {
            'path':'{{ti.xcom_pull(task_ids = "store_prices")}}'
        }
        
    )

    load_to_dw = sql.load_file(
        task_id='load_to_dw',
        input_file=File(
            path=f"s3://{bucket_name}/{{{{ti.xcom_pull(task_ids='get_formatted_csv')}}}}",
            conn_id="minio"
        ),
        output_table=Table(
            name='stock_market',
            conn_id='postgres',
            metadata=Metadata(schema='public')
        ),
        if_exists='replace',  # ✅ 这里才是对的地方
        load_options={
            "aws_access_key_id": BaseHook.get_connection("minio").login,
            "aws_secret_access_key": BaseHook.get_connection("minio").password,
            "endpoint_url": BaseHook.get_connection("minio").host
        }
    )




    is_api_available() >> get_stock_prices >> store_prices >>format_prices >> get_formatted_csv >> load_to_dw

dag_instance = stock_market_dag()

