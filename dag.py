import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta


import requests
import json


def open_session(ti):

    url = 'https://dm-em.informaticacloud.com/ma/api/v2/user/login'

    myobj = {
        "@type":"login",
        "username":"***",
        "password":"***"
    }

    x = requests.post(url, json = myobj)
    json_obj = x.json()

    session_id = json_obj["icSessionId"]
    print(session_id)
    
    ti.xcom_push(key='session_id', value=session_id)


def run_a_task(ti):

    url_job = 'https://emw1.dm-em.informaticacloud.com/saas/api/v2/job'
    
    session_id = ti.xcom_pull(key='session_id', task_ids='open_session')
        
    Headers = {"icSessionId": session_id}

    myobj = {
        "@type":"job",
        "taskId":"0119EH0Z000000000005",
        "taskType":"MTT"
        # "taskId":"0119EH0I000000000002",
        # "taskType":"DSS"
    }

    y = requests.post(url_job, headers=Headers, json = myobj)
    print(y)


def close_session():
    
    url_logout = "https://dm-em.informaticacloud.com/ma/api/v2/user/logout"

    myobj = {
        "@type":"logout"
    }
    z = requests.post(url_logout, json = myobj)
    print(z)
    print('dag is split in functions')
    return "session is closed"


default_args = {
    'owner': 'airflow',
    'retry_delay': timedelta(minutes=5),
    }


dag_python = DAG(
	dag_id = "pythonoperator_",
	default_args=default_args,
	schedule_interval='@once',	
	dagrun_timeout=timedelta(minutes=60),
	description='use case of python operator in airflow',
	start_date = airflow.utils.dates.days_ago(1))


open_session = PythonOperator(
    task_id='open_session', 
    python_callable=open_session, 
    dag=dag_python
    )

run_a_task = PythonOperator(
    task_id='run_a_task', 
    python_callable=run_a_task,
    dag=dag_python
    )

close_session = PythonOperator(
    task_id='log_out', 
    python_callable=close_session, 
    dag=dag_python
    )

open_session >> run_a_task >> close_session
