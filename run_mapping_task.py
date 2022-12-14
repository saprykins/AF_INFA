import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from airflow.utils.dates import days_ago


import requests
import json


def open_session(ti):
    # LOGIN TO INFA

    # Clients proposal start
    from airflow.hooks.base_hook import BaseHook
    connection = BaseHook.get_connection("informatica_service_account")
    password = connection.password 
    # the END

    # is used to get icSessionId
    url = 'https://dm-em.informaticacloud.com/ma/api/v2/user/login'

    myobj = {
        "@type":"login",
        "username":"ssaprykin",
        "password":password
    }

    # x is response from INFA
    x = requests.post(url, json = myobj)
    # make response as json to be able to read as dictionary
    json_obj = x.json()

    # informatica session id
    session_id = json_obj["icSessionId"]
    print(session_id)
    
    # session_id below is INFA session id
    ti.xcom_push(key='session_id', value=session_id)
    
    # return session_id


def run_a_task(ti):
    # START A TASK

    url_job = 'https://emw1.dm-em.informaticacloud.com/saas/api/v2/job'
    
    # get session_id from another function
    
    # accuracies = ti.xcom_pull(key='model_accuracy', session_id=['training_model_A', 'training_model_B', 'training_model_C'])
    session_id = ti.xcom_pull(key='session_id', task_ids='open_session')
        
    Headers = {"icSessionId": session_id}

    myobj = {
        "@type":"job",
        "taskId":"0119EH0I000000000002",
        "taskType":"DSS"
    }

    y = requests.post(url_job, headers=Headers, json = myobj)
    print(y)



def close_session():
    # CLOSE SESSION

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
    #'start_date': airflow.utils.dates.days_ago(2),
    # 'end_date': datetime(),
    # 'depends_on_past': False,
    #'email': ['airflow@example.com'],
    #'email_on_failure': False,
    #'email_on_retry': False,
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    #'retries': 1,
    'retry_delay': timedelta(minutes=5),
    }


dag_python = DAG(
	dag_id = "pythonoperator_",
	default_args=default_args,
	# schedule_interval='0 0 * * *',
	schedule_interval='@once',	
	dagrun_timeout=timedelta(minutes=60),
	description='use case of python operator in airflow',
	start_date = airflow.utils.dates.days_ago(1))


# dummy_task = DummyOperator(task_id='dummy_task', retries=3, dag=dag_python)

# python_task = PythonOperator(task_id='python_task', python_callable=my_func, dag=dag_python)

# log_in = PythonOperator(task_id='log_in', python_callable=log_in, dag=dag_python)

# let's try this
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

# log_in >> run_task >> log_out
# run_task

# let's try this
# open_session = open_session()
open_session >> run_a_task >> close_session

# two links to pass params
# https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/python.html
# https://stackoverflow.com/questions/54894418/how-to-pass-parameter-to-pythonoperator-in-airflow
