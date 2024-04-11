from airflow import DAG
import pendulum
from airflow.decorators import task

with DAG(
    dag_id="dags_python_task_decorator",
    schedule="0 2 * * 1",
    start_date=pendulum.datetime(2021,1,1,tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
    @task(task_id="python_task1")
    def print_context(some_input):
        print(some_input)

python_task1 = print_context("Python Task Decorator 실행")  # task_id와 task object명도 되도록 일치시키는게 좋음
