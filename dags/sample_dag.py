from email import message
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from random import randint  # Import to generate random numbers



def _bot_name():
    return  "I'm a bot"

def _hello(**context):
    ti = context['ti']
    xcom_value = ti.xcom_pull(task_ids="calling_bot")
    return f'hola {xcom_value}'

def _debug(**context):
    ti = context['ti']
    xcom_value = ti.xcom_pull(task_ids="hello_bot")
    print(xcom_value)


with DAG("a_hello_dag",  # Dag id
         # start date, the 1st of January 2021
         start_date=datetime(2021, 1, 1),
         # Cron expression, here it is a preset of Airflow, @daily means once every day.
         schedule_interval='@daily',
         catchup=False  # Catchup
         ) as dag:
    
    # Tasks are implemented under the dag object
    calling_bot = PythonOperator(
        task_id="calling_bot",
        python_callable=_bot_name
    )

    # hello_default = PythonOperator(
    #     task_id="hello_bot",
    #     python_callable=_hello,
    #     op_kwargs={"name": "owner"},
    # )
    
    hello_bot = PythonOperator(
        task_id="hello_bot",
        python_callable=_hello,
        provide_context=True,         
        dag=dag,
    )

        
    debug = PythonOperator(
        task_id="debug",
        python_callable=_debug,
        provide_context=True,         
        dag=dag,
    )

    calling_bot >> hello_bot >> debug
