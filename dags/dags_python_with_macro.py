from airflow import DAG
import pendulum
from airflow.decorators import task

with DAG(
    dag_id="dags_python_with_macro",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2024, 3, 30, tz="Asia/Seoul"),
    catchup=False,
) as dag:

    @task(
        task_id="task_using_macros",
        templates_dict={
            "start_date": "{{ (data_interval_start.in_timezone('Asia/Seoul') + macros.dateutil.relativedelta.relativedelta(months=-1, day=1)) | ds }}",
            "end_date": "{{ (data_interval_start.in_timezone('Asia/Seoul').replace(day=1) + macros.dateutil.relativedelta.relativedelta(days=1)) | ds }}",
        },
    )
    def get_datetime_macro(**kwargs):
        # task 데코레이터의 키워드 인자들이 key:value 형태의 딕셔너리로 들어감
        # kwargs.get('templates_dict') -> templates_dict 인자의 값으로 들어간 딕셔너리를 반환
        # 그 값이 없으면 빈 딕셔너리 반환
        templates_dict = kwargs.get("templates_dict") or {}
        if templates_dict:
            start_date = templates_dict.get("start_date") or "start_date 없음"
            end_date = templates_dict.get("end_date") or "end_date 없음"
            print(start_date)
            print(end_date)

    @task(task_id="task_direct_calc")
    def get_datetime_calc(**kwargs):

        # 스케쥴러 부하를 막기 위해 특정 태스크에서만 사용되는 라이브러리/모듈은 태스크 안에서 임포트
        # 태스크/오퍼레이터 안에 있는 내용들은 대그 파싱할때 검사하지 않으므로 스케쥴러의 부하를 줄일 수 있음
        # dag 선언문 바깥이나 태스크 바깥에 있는 내용들은 에어플로우에서 대그가 돌지 않아도 매번 파싱을 수행함
        from dateutil.relativedelta import relativedelta

        data_interval_end = kwargs["data_interval_end"]
        prev_month_day_first = data_interval_end.in_timezone(
            "Asia/Seoul"
        ) + relativedelta(month=-1, day=1)
        prev_month_day_last = data_interval_end.in_timezone("Asia/Seoul").replace(
            day=1
        ) + relativedelta(days=-1)
        print(prev_month_day_first.strftime("%Y-%m-%d"))
        print(prev_month_day_last.strftime("%Y-%m-%d"))

    get_datetime_macro() >> get_datetime_calc()
