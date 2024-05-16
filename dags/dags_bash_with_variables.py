from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
import pendulum

with DAG(
    dag_id="dags_bash_with_variables",
    schdeule="10 9 * * *",
    start_date=pendulum.datetime(2023, 4, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    # 1. Variable 라이브러리를 이용한 전역변수 호출
    var_value = Variable.get("sample_key")
    bash_var_1 = BashOperator(
        task_id="bash_var_1", bash_command=f"echo variable:{var_value}"
    )

    # 2. Jinja template을 이용한 전역변수 호출
    bash_var_2 = BashOperator(
        task_id="bash_var_2", bash_command="echo varialbe:{{var.value.sample_key}}"
    )
