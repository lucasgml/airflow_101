from prophet import *

from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

# Defining default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['lgmleite@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=30)
}

dag = DAG(
    'covid_cases',
    default_args=default_args,
    description='A DAG for sending daily predictions for Covid-19 cases',
    schedule_interval=timedelta(days=1)
)

download_task = PythonOperator(
    task_id='download_data',
    python_callable=download_data,
    dag=dag,
)

unzip_task = PythonOperator(
    task_id='unzip_data',
    python_callable=unzip_data,
    dag=dag,
)

folder_task = PythonOperator(
    task_id='create_folder',
    python_callable=create_fig_folder,
    dag=dag,
)

generate_task = PythonOperator(
    task_id='generate_data',
    python_callable=generate_graphs,
    dag=dag,
)

email_task = EmailOperator(
    task_id='email_sender',
    to=['lgmleite@gmail.com'],
    subject='Covid prediction - Airflow Test',
    files=['../data/figs/city_Fortaleza.png','../data/figs/city_São Paulo.png','../data/figs/state_CE.png','../data/figs/state_SP.png'],
    html_content="""
    <h3>Covid-19 predictions</h3>
    
    Please find attached data for the next 15 days for the cities of Fortaleza and Sao Paulo and the states of Ceara and Sao Paulo.
    """,
    dag=dag
)

delete_task = PythonOperator(
    task_id='delete_data',
    python_callable=delete_file,
    dag=dag,
)

download_task >> unzip_task
unzip_task >> folder_task
folder_task >> generate_task
generate_task >> email_task
email_task >> delete_task