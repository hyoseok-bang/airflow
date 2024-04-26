from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator
from airflow.decorators import task

with DAG(
    dag_id="dags_bash_python_with_xcom",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2024, 4, 15, tz="Asia/Seoul"),
    catchup=False,
) as dag:

    @task(task_id="python_push")
    def python_push_xcom():
        result_dict = {"status": "Good", "data": [1, 2, 3], "options_cnt": 100}
        return result_dict

    bash_pull = BashOperator(
        task_id="bash_pull",
        env={
            "STATUS": "{{ ti.xcom_pull(task_id='python_push')['status'] }}",  # Good
            "DATA": "{{ ti.xcom_pull(task_id='python_push)['data'] }}",  # [1,2,3]
            "OPTIONS_CNT": "{{ ti.xcom_pull(task_id='python_push)['options_cnt'] }}",  # 100
        },
        bash_command="echo $STATUS && echo $DATA && echo $OPTION_CNT",
    )

    python_push_xcom() >> bash_pull

    # From Bash to Python
    bash_push = BashOperator(
        task_id="bash_push",
        bash_command="echo PUSH_START "
        '{{ ti.xcom_push(key="bash_pushed", value=200) }} && '
        "echo PUSH_COMPLETE",
    )

    @task(task_id="python_pull")
    def python_pull_xcom(**kwargs):
        ti = kwargs["ti"]
        status_value = ti.xcom_pull(key="bash_pushed")  # 200
        return_value = ti.xcom_pull(task_ids="bash_push")  # PUSH_COMPLETE
        print("status_value: " + str(status_value))
        print("return_value: " + return_value)

    bash_push >> python_pull_xcom()
