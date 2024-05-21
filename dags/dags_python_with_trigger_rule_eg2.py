from airflow import DAG
from airflow.decorators import task
import pendulum

with DAG(
    dag_id="dags_branch_task_decorator",
    schedule="9 10 * * *",
    start_date=pendulum.datetime(2024, 5, 10, tz="Asia/Seoul"),
) as dag:

    @task.branch(task_id="python_branch_task")
    def random_branch():
        import random

        item_list = ["A", "B", "C"]
        selected_item = random.choice(item_list)
        if selected_item == "A":
            return "task_a"
        if selected_item == "B":
            return "task_b"
        if selected_item == "C":
            return "task_c"

    @task(task_id="task_a")
    def task_a():
        print("Task Succeed!")

    @task(task_id="task_b")
    def task_b():
        print("Task Succeed!")

    @task(task_id="task_c")
    def task_c():
        print("Task Succeed!")

    @task(task_id="task_d", trigger_rule="non_skipped")
    def task_d():
        print("Task Succeed!")

    random_branch() >> [task_a(), task_b(), task_c()] >> task_d()
