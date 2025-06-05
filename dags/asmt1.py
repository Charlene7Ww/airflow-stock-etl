from airflow.decorators import dag, task
from datetime import datetime, timedelta
import random
 

@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    description='A simple DAG to generate and check random numbers',
    catchup=False
) 
def random_number_checker():

    @task
    def generate_random_number():
        number = random.randint(1, 100)
        # ti.xcom_push(key='random_number', value=number)
        print(f"Generated random number: {number}")
        return number
    
    @task
    def check_even_odd(value):
        # number = ti.xcom_pull(task_ids='generate_number', key='random_number')
        result = "even" if value % 2 == 0 else "odd"
        print(f"The number {value} is {result}.")

    number = generate_random_number()
    check_even_odd(number)

#在 Airflow 的 TaskFlow API 中，每个 @task 应该独立定义，然后通过 返回值依赖 建立关联，而不是直接嵌套调用。


random_number_checker = random_number_checker()

