"""Example DAG demonstrating the usage of the BashOperator."""

from datetime import date, datetime, timedelta, tzinfo
import pytz
import pendulum

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
import pandas as pd

tz = pendulum.timezone('America/Argentina/Buenos_Aires')

default_args = {
    'owner': 'jf23perez',
    'depends_on_past': False,
    'email': ['jf23perez@gmail.com'],
    'email_on_failure': False,
    'retries': 1,
    'tzinfo': tz,
    'start_date': datetime(2021, 12, 1, tzinfo=tz)
}
dag = DAG(
    'beek_test_etl',
    default_args=default_args,
    schedule_interval='0 6 * * *',
    catchup=False
)

def get_csv(**kwargs):
    mysqlserver = MySqlHook('mysql_beek')
    sql = """
        SELECT 
            u.id AS user_id, 
            p.id AS payment_id, 
            s.id AS subscription_id,
            u.user_name, 
            s.status, 
            s.cancellation_intent, 
            s.subscription_started_at, 
            p.payment_status, 
            p.payment_date
        FROM
            beek_test.audiobook_subscriptions s
        INNER JOIN
            beek_test.payment p
        ON
            s.payment_id = p.id
        INNER JOIN
            beek_test.users u
        ON
            s.user_id = u.id
    """
    df = mysqlserver.get_pandas_df(sql=sql)
    df.to_csv("/home/airflow/data/outputT.csv", index=False)
    print(df)

beek_etl = PythonOperator(
    dag=dag,
    task_id="select_csv",
    python_callable=get_csv
)

beek_etl