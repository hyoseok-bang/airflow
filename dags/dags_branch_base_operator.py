from tracemalloc import start
from typing import Iterable
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.branch import BaseBranchOperator
from airflow.utils.context import Context
import pendulum

with DAG(
    dag_id="dags_branch_base_operator",
    schedule="9 10 * * *",
    start_date=pendulum.datetime(2024, 5, 10, tz="Asia/Seoul"),
    catchup=False,
) as dag:

    class CustomBranchOperator(BaseBranchOperator):
        def choose_branch(self, context: Context) -> str | Iterable[str]:
            import random

            print(context)  # 변수 내부 내용 확인용
            item_list = ["A", "B", "C"]
            selected_item = random.choice(item_list)
            if selected_item == "A":
                return "task_a"
            else:
                return ["task_b", "task_c"]

    def common_func(**kwargs):
        print(kwargs["selected"])

    task_a = PythonOperator(
        task_id="task_a", python_callable=common_func, op_kwargs={"selected": "A"}
    )
    task_b = PythonOperator(
        task_id="task_b", python_callable=common_func, op_kwargs={"selected": "B"}
    )
    task_c = PythonOperator(
        task_id="task_c", python_callable=common_func, op_kwargs={"selected": "C"}
    )
    custom_branch_operator = CustomBranchOperator(task_id="python_branch_task")

    custom_branch_operator >> [task_a, task_b, task_c]
