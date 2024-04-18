from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_macro_eg1",
    schedule="10 0 * * 6#2",  # 매월 둘째주 토요일 00시 10분마다 실행
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:

    # START_DATE: 전월 말일, END_DATE: 1일 전
    bash_task_1 = BashOperator(
        task_id="bash_task_1",
        env={
            # START_DATE == 바로 이전 배치가 돌았던 날짜 == data_interval_start
            # in_timezone 메소드로 원하는 타임존 시간으로 출력 가능 (에어플로우 시간은 디폴트로 UTC 기준)
            "START_DATE": "{{ data_interval_start.in_timezone('Asia/Seoul') | ds }}",
            "END_DATE": "{{ (data_interval_end.in_timezone('Asia/Seoul') - macros.dateutil.relativedelta.relativedelta(dyas=1)) | ds }}",
        },
        bash_command="echo 'START_DATE: $START_DATE' && echo 'END_DATE: $END_DATE'",
    )
