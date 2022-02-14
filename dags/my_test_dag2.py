from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'pipis',
    'start_date': days_ago(1)
}
 
dag = DAG(dag_id = 'my_sample_dag', default_args=args, schedule_interval='@daily')
 
 
def run_this_func():
    print('Sacar info del storage')

def run_this_func_2():
    print('Procesar datos')
 
def run_also_this_func():
    print('Subir datos a otro lado')
 
 
with dag:
    run_this_task_1 = PythonOperator(
        task_id='extract_data_1',
        python_callable = run_this_func
    )
    run_this_task_2 = PythonOperator(
        task_id='extract_data_2',
        python_callable = run_this_func
    )
    run_this_task_3 = PythonOperator(
        task_id='extract_data_3',
        python_callable = run_this_func
    )
    run_this_task_4 = PythonOperator(
        task_id='extract_data_4',
        python_callable = run_this_func
    )

    run_this = PythonOperator(
        task_id='process_data',
        python_callable = run_this_func
    )
 
    run_this_task_too = PythonOperator(
        task_id='upload_data',
        python_callable = run_also_this_func
    )
 
    [run_this_task_1, run_this_task_2, run_this_task_3, run_this_task_4] >> run_this >> run_this_task_too