# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.python import PythonOperator

# default_args = {
#     'owner': 'haii',
#     'retries': 5,
#     'retry_delay': timedelta(minutes=5)
# }


# def get_name(ti):
#     ti.xcom_push(key='first_name', value='Jerry')
#     ti.xcom_push(key='last_name', value='Fridman')


# def greet(ti):
#     first_name = ti.xcom_pull(task_ids='get_name', key='first_name')
#     last_name = ti.xcom_pull(task_ids='get_name', key='last_name')
#     print(f"Hello world {first_name}, {last_name}")


# with DAG(
#         default_args=default_args,
#         dag_id='python_with_operator_v05',
#         description='Our first dag using python operator',
#         start_date=datetime(2023, 12, 4),
#         schedule_interval='@daily'
# ) as dag:
#     task1 = PythonOperator(
#         task_id='greet',
#         python_callable=greet
#     )

#     task2 = PythonOperator(
#         task_id='get_name',
#         python_callable=get_name
#     )

#     task2 >> task1
