from airflow import DAG
import pendulum
import datetime
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_xcom",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2024, 4, 15, tz="Asia/Seoul"),
    catchup=False,
) as dag:

    # Push Task
    bash_push = BashOperator(
        task_id="bash_push",
        bash_command="echo START && "
        "echo XCOM_PUSHED "
        "{{ ti.xcom_push(key='bash_pushed', value='first_bash_message') }} && "  # Save bash_pushed:'first_bash_message' pair in XCOM meta DB
        "echo COMPLETE",
    )

    # Pull Task
    bash_pull = BashOperator(
        task_id="bash_pull",
        env={
            "PUSHED_VALUE": "{{ ti.xcom_pull(key='bash_pushed') }}",  # Return 'first_bash_message'
            "RETURN_VALUE": "{{ ti.xcom_pull(task_id='bash_push') }}",  # Return COMPLETE
        },
        bash_command="echo $PUSHED_VALUE && echo $RETURN_VALUE",
        do_xcom_push=False,  # If True(default), register return value of the task as the value of return_value key of task instance
    )

    bash_push >> bash_pull
