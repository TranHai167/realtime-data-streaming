from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'Hai Tran',
    'start_date': datetime(2024, 1, 8)
}


def get_data():
    import requests
    res = requests.get('https://randomuser.me/api/')
    res = res.json()
    res = res['results'][0]
    return res


def format_data(res):
    data = {}
    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])}  {location['street']['name']}" \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['postcode'] = location['postcode']
    return data


def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging

    # print(json.dumps(res, indent=3))
    producer = KafkaProducer(bootstrap_servers=['localhost:9092', 'broker:29092'],api_version=(2,0,2))
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60: break  # 1 minute
        try:
            res = get_data()
            res = format_data(res)
            producer.send('my_topic', value=json.dumps(res).encode('utf-8'))
        except Exception as e:
            logging.error('An error occurs: {e}')
            continue
    producer.close()


with DAG(
    dag_id='data-airflow',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )

    


