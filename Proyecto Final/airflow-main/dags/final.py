import os
#Librerias utilizadas AIRFLOW, PANDAS y structlog
from airflow import DAG
from airflow.contrib.hooks.fs_hook import FSHook
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from structlog import get_logger
import pandas as pd
pd.options.mode.chained_assignment = None

logger = get_logger()
# conexiones a crear en el Airflow
FILE_CONNECTION_NAME = 'my_file_path' #Lugar donde deben de residir los archivos de entrada
CONNECTION_DB_NAME = 'mysql_con' # Conexion a la base de datos.

#configuracion general del DAG para el examen final.
dag = DAG('final_dag', description='This is a new implementation of final dag',
          default_args={
              'owner': 'Jairo Salazar',
              'depends_on_past': False,
              'max_active_runs': 1,
              'start_date': days_ago(5)
          },
          schedule_interval='0 1 * * *',
          catchup=True)

#filetodb
#Descripcion: Este procedimiento es llamado tres veces desde los Dags de ETL, una vez por cada archivo.
#Parametros: table_name -> nombre de la tabla a crear en la base de datos
#            file_name1 -> nombre del archivo a leer del directorio my_file_path
#            TypeofFile -> Tipo de archivo: Confirmed, Death, Recovered
#Flujo:   1. Crear Conexion hacia el Mysql
#         2. Leer y cargar archivo csv, el nombre del archivo viaja en el parametro  file_name1
#         3. Transformar el archivo y agruparlo por: province,country,lat,long,date, transformando con la funcion MELT
#            de pandas las columnas en filas por fecha.
#         4. Recorrer el dataset por pais, para determinar el delta (cambio de un dia a otro) en el numero de casos.
#         5. Si es el primer DAG (confirmed) se crea la tabla final cases_cv19
#         6. Insertar datos del archivo leido en la tabla trasladada en el parametro table_name, una vez se transformo
#            el dataset.
#         7. Efectuar el insert final sobre la tabla cases_cv19 a partir de los datos agregados a la tabla en el punto
#            anterior.
def filetodb(table_name, file_name1,TypeofFile):
    file_path = FSHook(FILE_CONNECTION_NAME).get_path()
    filename = file_name1
    mysql_connection = MySqlHook(mysql_conn_id=CONNECTION_DB_NAME).get_sqlalchemy_engine()
    full_path = f'{file_path}/{filename}'
    data = pd.read_csv(full_path, encoding="ISO-8859-1")
    datafinal = data.melt(id_vars=["Province/State", "Country/Region", "Lat", "Long"],
                          var_name="Date",
                          value_name="Confirmed")
    datafinal.columns = ["Province/State", "Country/Region", "Lat", "Long", "Date", "CasesOrig"]#,"NewCases","Type"]
    #Transformation
    dffinal2 = pd.DataFrame(columns=["Province/State", "Country/Region", "Lat", "Long", "Date", "CasesOrig", "Cases", "Status","Year-month"])
    uniqueValues = datafinal["Country/Region"].unique()
    for row in uniqueValues:
        Country = datafinal.loc[datafinal['Country/Region'] == row]
        Country["Province/State"].fillna("TODO", inplace=True)
        uniqueValues2 = Country["Province/State"].unique()
        for row2 in uniqueValues2:
            region = Country.loc[Country["Province/State"]==row2]
            region['Cases'] = region['CasesOrig'] - region.shift(periods=1, fill_value=0)['CasesOrig']
            region['Status'] = TypeofFile
            region['Year-month'] = pd.to_datetime(Country['Date']).dt.strftime('%Y-%m')
            dffinal2 = dffinal2.append(region)
        #Country['Cases'] = Country['CasesOrig'] - Country.shift(periods=1, fill_value=0)['CasesOrig']
        #Country['Status'] = TypeofFile
        #Country['Year-month'] = pd.to_datetime(Country['Date']).dt.strftime('%Y-%m')
        #dffinal2 = dffinal2.append(Country)

    print(dffinal2)

    with mysql_connection.begin() as connection:
        #connection.execute(f'DELETE FROM {table_name} WHERE 1=1')
        if TypeofFile == 'Confirmed':
            dffinal3 = pd.DataFrame(columns=["Province/State", "Country/Region", "Lat", "Long", "Date", "CasesOrig", "Cases", "Status","Year-month"])
            dffinal3.to_sql('cases_cv19', con=connection, schema='test', if_exists='replace', index=False)
        dffinal2.to_sql(table_name, con=connection, schema='test', if_exists='replace', index=False)
        #connection.execute("insert into cases_cv19 select * from " + table_name)
        connection.execute(f'USE test; insert into cases_cv19 select * from {table_name}')

#Procedimiento: etl
# Funcion: cargar archivo time_series_covid19_confirmed_global a la tabla cases_confirmedCV19 por medio de la funcion filetodb

def etl(**kwargs):
    filetodb('cases_confirmedCV19','time_series_covid19_confirmed_global.csv','Confirmed')

#Procedimiento: etl2
# Funcion: cargar archivo time_series_covid19_recovered_global a la tabla cases_recoveredCV19 por medio de la funcion filetodb

def etl2(**kwargs):
    filetodb('cases_recoveredCV19','time_series_covid19_recovered_global.csv','Recovered')

#Procedimiento: etl1
# Funcion: cargar archivo time_series_covid19_deaths_global a la tabla cases_deathCV19 por medio de la funcion filetodb

def etl1(**kwargs):
    filetodb('cases_deathCV19','time_series_covid19_deaths_global.csv','Death')




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
sensor2 = FileSensor(task_id='final_sensor_file2',
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