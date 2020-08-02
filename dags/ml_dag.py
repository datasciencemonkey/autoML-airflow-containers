from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.email_operator import EmailOperator

from workflow_func import cas_docker_connector
from workflow_func import enterprise_viya_aws_connector
from workflow_func import enterprise_viya_race_connector
from workflow_func import prep_data
from workflow_func import call_cas_automl
from workflow_func import score_and_download_artifacts
from workflow_func import push_model_artifacts_x_enterprise_model_manager

today_date = datetime.strftime(datetime.now(), '%Y-%m-%d')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 6, 2),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('ml_dag',default_args=default_args,schedule_interval='@weekly', template_searchpath=['/usr/local/airflow/sql_files'], catchup=True) as dag:

    t1=BashOperator(task_id='check_file_exists', bash_command='shasum ~/store_files_airflow/churn_xl.parquet', retries=2, retry_delay=timedelta(seconds=15))

    # t2=BashOperator(task_id='check_bashrc_spit_conda', bash_command='cd && ls -al && source .bashrc', retries=1)

    t2 = PythonOperator(task_id='check_cas_connection', python_callable=cas_docker_connector)

    t3 = PythonOperator(task_id='prep_data_abt', python_callable=prep_data)

    t4 = PythonOperator(task_id='summon_automl_magic',python_callable=call_cas_automl)

    # TODO: Check if the function reflects the right Viya Environment. AWS/ex-net AWS
    t5 = PythonOperator(task_id='check_enterprise_viya_conn', python_callable=enterprise_viya_race_connector)

    t6 = PythonOperator(task_id='score_download_artifacts', python_callable=score_and_download_artifacts)

    t7 = EmailOperator(task_id='send_email',
        to='sathish.gangichetty@sas.com',
        subject=f'Weekly AutoML Run Complete - Execution Date - {today_date}',
        html_content=f""" <h1> AutoML Reporting! While you were watching Dark all day, I worked out new models and found a new champion.</h1><p>Tsk... Don't forget to check the attachment.. :-) </p> """,
        files=['/usr/local/airflow/store_files_airflow/topNPipeLines.csv'])
    
    t8 =  PythonOperator(task_id='push_model_artifacts_to_model_manager', python_callable=push_model_artifacts_x_enterprise_model_manager)


    t1 >> t2 >> t3 >> t4 >> [t5,t6] >> t7 >> t8
