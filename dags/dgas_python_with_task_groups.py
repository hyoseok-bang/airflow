from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task, task_group
from airflow.utils.task_group import TaskGroup
import pendulum

with DAG(
    dag_id="dags_python_with_task_groups",
    schedule="9 10 * * *",
    start_date=pendulum.datetime(2024, 5, 20, tz="Asia/Seoul"),
    catchup=False,
) as dag:

    def inner_func(**kwargs):
        msg = kwargs.get("msg") or ""
        return msg

    @task_group(group_id="first_group")
    def group_1():
        """First task group"""

        @task(task_id="inner_function1")
        def inner_func1(**kwargs):
            print("First task of the first task group.")

        inner_function2 = PythonOperator(
            task_id="inner_function2",
            python_callable=inner_func,
            op_kwargs={"msg": "Second task of the first task group."},
        )

        inner_func1() >> inner_function2

    with TaskGroup(group_id="second_group", tooltip="Second task group") as group_2:
        """
        docstring in TaskGroup class doesn't show up in Airflow UI
        value of tooltip parameter shows up instead
        """

        @task(task_id="inner_function1")
        def inner_func1(**kwargs):
            print("First task of the second task group.")

        inner_function2 = PythonOperator(
            task_id="inner_function2",
            python_callable=inner_func,
            op_kwargs={"msg": "Second task of the second task group."},
        )

        inner_func1() >> inner_function2

    group_1() >> group_2
