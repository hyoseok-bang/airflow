from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from airflow.exceptions import AirflowException
import pendulum

with DAG(
    dag_id="dags_python_with_trigger_rule_eg1",
    schedule="9 10 * * *",
    start_date=pendulum.datetime(2024, 5, 20, tz="Asia/Seoul"),
    catchup=False,
) as dag:

    bash_upstream_1 = BashOperator(
        task_id="bash_upstream_1", bash_command="echo upstream1"
    )

    @task(task_id="python_upstream_1")
    def python_upstream_1():
        raise AirflowException("downstream_1 Exception")  # Mark as failed

    @task(task_id="python_upstream_2")
    def python_upstream_2():
        print("Task Succeed!")

    @task(task_id="python_downstream_1", trigger_rule="all_done")
    def python_downstream_1():
        print("Task Succeed!")

    [bash_upstream_1, python_upstream_1(), python_upstream_2()] >> python_downstream_1()
