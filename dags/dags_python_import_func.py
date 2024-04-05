from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
from common.common_func import get_sftp  # 로컬에서 실행할 경우 improt error; 최상위 경로가 Airflow/local_repo로 되어있기 때문에 plugins.common.common_func로 해주어야함
                                         # 하지만 airflow container에는 plugins 폴더가 파이썬 패쓰로 등록되어있어서 plugins를 포함하면 에러남
                                         # .env 파일 생성하여 plugins 폴더까지 Python Path로 등록하면 문제 해결

with DAG(
    dag_id="dags_python_import_func",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    task_get_sftp = PythonOperator(
        task_id="task_get_sftp",
        python_callable=get_sftp
    )
    
    task_get_sftp