import datetime
import pendulum
# from airflow import DAG
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
import random


with DAG(
    dag_id="dags_python_operator",  # 에어플로우 대시보드 상에서 보여지는 dag 이름; python 파일명과는 상관없지만, 일치시키는게 관리하기에 좋음
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),
    catchup=False,  # 현재 날짜와 start_date보다 이후일떄, 누락된 날짜들까지 소급해서 돌릴건지 결정
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["example", "example2"],
    # params={"example_key": "example_value"},  # dag 안에 설정된 태스크들이 공통적으로 가져갈 파라미터들
) as dag:
    def select_fruit():
        fruit = ['banana', 'orange', 'apple', 'avocado']
        rand_int = random.randint(0,3)
        print(fruit[rand_int])
    
    py_t1 = PythonOperator(
        task_id='py_t1',
        python_callable=select_fruit
    )
    
    py_t1