from airflow import DAG
import pendulum
from airflow.decorators import task

with DAG(
    dag_id="dags_python_with_xcom_eg1",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:

    # 태스크 간 데이터 전달 방식 2: 태스크 함수에서 리턴값을 직접 반환
    @task(task_id="python_xcom_push_by_return")
    def xcom_push_by_return(**kwargs):
        return "Success"

    # 이전 태스크의 리턴값 받는 방법 1: xcom_pull 메서드의 return_value 키를 통해
    @task(task_id="python_xcom_pull_1")
    def xcom_pull_1(**kwargs):
        ti = kwargs["ti"]
        value1 = ti.xcom_pull(
            task_id="python_xcom_push_by_return"
        )  # task_ids값 넣으면 알아서 리턴값 받아줌
        print("xcom_pull 메서드로 직접 찾은 리턴 값:", value1)

    # 이전 태스크의 리턴값 받는 방법 2: 리턴값을 갖는 태스크 자체를 인풋으로 전달
    @task(task_id="python_xcom_pull_2")
    def xcom_pull_2(status, **kwargs):
        print("함수 입력값으로 받은 값:", status)

    xcom_push_return_value = (
        xcom_push_by_return()
    )  # xcom_push_return_value는 단순히 str값을 저장한 변수가 아니라 airflow task 객체
    xcom_push_return_value >> xcom_pull_1()
    xcom_pull_2(xcom_push_by_return())  # 리턴값을 갖는 태스크 객체를 입력으로 전달
