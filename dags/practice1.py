from airflow.decorators import dag, task
from datetime import datetime

@dag(start_date=datetime(2024, 1, 1),
     schedule="@daily",
     description='Deepseek practice1',
     catchup=False
)

def practice1():

    @task
    def first_number():
        number = 1
        print("First number is:", number)
        return number
    @task
    def second_number(value):
        number2 = value+1
        print("Second number is:", number2)

    number = first_number()
    second_number(number)

practice1()