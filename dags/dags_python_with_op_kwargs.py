from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from common.common_func import regist2

with DAG(
    dag_id="dags_python_with_op_kwargs",
    schedule="0 2 * * 1",
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:

    regist_t1 = PythonOperator(
        task_id="regist_t1",
        python_callable=regist2,
        op_args=["hyseok_bang", "man", "kr", "seoul"],
        op_kwargs={"phone": "010-1234-1234"},
    )

    regist_t1
