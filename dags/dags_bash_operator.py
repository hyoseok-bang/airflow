import datetime
import pendulum
# from airflow import DAG
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="dags_bash_operator",  # 에어플로우 대시보드 상에서 보여지는 dag 이름; python 파일명과는 상관없지만, 일치시키는게 관리하기에 좋음
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),
    catchup=False,  # 현재 날짜와 start_date보다 이후일떄, 누락된 날짜들까지 소급해서 돌릴건지 결정
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["example", "example2"],
    params={"example_key": "example_value"},  # dag 안에 설정된 태스크들이 공통적으로 가져갈 파라미터들
) as dag:
    # [START howto_operator_bash]
    bash_t1 = BashOperator(
        task_id="run_after_loop",  # dag 그래프 상에 보여지는 태스크명
        bash_command="echo bashoperator",  # 실행할 쉘스크립트 명령어
    )
    
    bash_t2 = BashOperator(
        task_i="bash_t2",
        bash_command="echo $HOSTNAME"  # 태스크가 실행되는 
    )
    
    bash_t1 >> bash_t2 