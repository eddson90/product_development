import os

from airflow import DAG
from airflow.contrib.hooks.fs_hook import FSHook
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from structlog import get_logger
import pandas as pd

logger = get_logger()
FILE_CONNECTION_NAME = 'monitor_file'
CONNECTION_DB_NAME = 'mysql2'

dag = DAG('final_dag', description='This is a new implementation of final dag',
          default_args={
              'owner': 'Jairo Salazar',
              'depends_on_past': False,
              'max_active_runs': 5,
              'start_date': days_ago(5)
          },
          schedule_interval='0 1 * * *',
          catchup=True)

def filetodb(table_name,file_name1):
    file_path = FSHook(FILE_CONNECTION_NAME).get_path()
    filename = file_name1
    mysql_connection = MySqlHook(mysql_conn_id=CONNECTION_DB_NAME).get_sqlalchemy_engine()
    full_path = f'{file_path}/{filename}'
    data = pd.read_csv(full_path, encoding="ISO-8859-1")
    datafinal = data.melt(id_vars=["Province/State", "Country/Region", "Lat", "Long"],
                          var_name="Date",
                          value_name="Confirmed")
    datafinal.columns = ["Province", "Country", "Lat", "Long", "Date", "Confirmed"]

    with mysql_connection.begin() as connection:
        connection.execute(f'DELETE FROM {table_name} WHERE 1=1')
        df.to_sql(table_name, con=connection, schema='test', if_exists='append', index=False)


def etl(**kwargs):
    filetodb('test.cases_confirmedCV19','time_series_covid19_confirmed_global.csv')

def etl2(**kwargs):
    filetodb('test.cases_recoveredCV19','time_series_covid19_recovered_global.csv')

def etl1(**kwargs):
    filetodb('test.cases_deathCV19','time_series_covid19_deaths_global.csv')
    #file_path = FSHook(FILE_CONNECTION_NAME).get_path()
    #filename = 'time_series_covid19_confirmed_global.csv'
    #mysql_connection = MySqlHook(mysql_conn_id=CONNECTION_DB_NAME).get_sqlalchemy_engine()
    #full_path = f'{file_path}/{filename}'
    #data = pd.read_csv(full_path, encoding="ISO-8859-1")
    #datafinal = data.melt(id_vars=["Province/State", "Country/Region", "Lat", "Long"],
    #                      var_name="Date",
    #                      value_name="Confirmed")
    #datafinal.columns = ["Province", "Country", "Lat", "Long", "Date", "Confirmed"]

    #with mysql_connection.begin() as connection:
    #    connection.execute("DELETE FROM test.cases_confirmedCV19 WHERE 1=1")
    #    df.to_sql('cases_confirmedCV19', con=connection, schema='test', if_exists='append', index=False)

sensor = FileSensor(task_id='final_sensor_file',
                    dag=dag,
                    fs_conn_id='my_file_path',
                    filepath='time_series_covid19_confirmed_global.csv',
                    poke_interval=5,
                    timeout=60)

etlOperator = PythonOperator(task_id="final_etl",
                     provide_context=True,
                     python_callable=etl,
                     dag=dag
                     )

sensor1 = FileSensor(task_id='final_sensor_file1',
                    dag=dag,
                    fs_conn_id='my_file_path',
                    filepath='time_series_covid19_deaths_global.csv',
                    poke_interval=5,
                    timeout=60)

etlOperator1 = PythonOperator(task_id="final_etl1",
                     provide_context=True,
                     python_callable=etl1,
                     dag=dag
                     )
sensor2 = FileSensor(task_id='final_sensor_file1',
                    dag=dag,
                    fs_conn_id='my_file_path',
                    filepath='time_series_covid19_recovered_global.csv',
                    poke_interval=5,
                    timeout=60)

etlOperator2 = PythonOperator(task_id="final_etl2",
                     provide_context=True,
                     python_callable=etl2,
                     dag=dag
                     )


sensor >> etlOperator >> sensor1 >> etlOperator1 >> sensor2 >> etlOperator2
