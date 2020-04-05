from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
from pymongo import MongoClient

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['cr13@correo.ugr.es'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}


#Inicialización del grafo DAG de tareas para el flujo de trabajo
dag = DAG(
    'workflow_p2',
    default_args=default_args,
    description='Grafo de tareas para prediccion de temperatura y humedad en San Francisco',
    schedule_interval=timedelta(days=1),
)

# Definimos el directorio donde se va han a clonar los microservicios
cc_p2dir = '~/cc_p2/'
temporaldir = '/tmp/workflow_p2/'

PrepararEntorno = BashOperator(
    task_id='PrepEntorno',
    depends_on_past=False,
    bash_command='mkdir -p ' + temporaldir + '',      # argumento -p para crear solo en caso de que no exista
    dag=dag,
)

# Se extrae la columna correspondiente a la fecha (date) y la ciudad concreta SAN FRANCISCO
# una vez limpiado los datos se van a unificar 
def select_unific_dat(csv_t,csv_h):
    # csv_t = temporaldir + 'temperature.csv'
    # csv_h = temporaldir + 'humidity.csv'
    try:
        df_temp = pd.read_csv(csv_t, sep=',', usecols=("datetime",
                                            "San Francisco"))
        df_hum = pd.read_csv(csv_h, sep=',', usecols=("datetime",
                                            "San Francisco"))

        df_temp.rename(columns={"San Francisco": "TEMP"}, inplace=True)
        df_hum.rename(columns={"San Francisco": "HUM"}, inplace=True)
        # Formato DATE;TEMP;HUM
        df = pd.merge(df_temp, df_hum, on='datetime')
        df.rename(columns={"datetime": "DATE"}, inplace=True)
        # Eliminar las filas con nulos
        df.dropna(inplace= True) 
        df.dropna(subset=['TEMP', 'HUM'], inplace= True)
        df.to_csv('' + temporaldir + 'datos_san_francisco.csv',  header=True, index=False, sep=',',  encoding="utf-8")
        # print("----------contenido----producto cartesiano------")
        # print(df.to_string())
        # print(df.to_dict('dict'))
        # try:
        #     client = MongoClient('localhost', 27017)
        #     db = client.meteosat_db
        #     # serverStatusResult=db.command("serverStatus")
        #     # print(serverStatusResult)
        #     result = db['predicciones'].insert_one({'index': 'meteosat_db', 'data': df.to_dict('dict')})
        #     client.close()
        # except:
        #     print("Error al guardar en bd")
    except:
        print("Error al procesar los ficheros csv")

CapturaDatosHum = BashOperator(
    task_id='CapturaDatosHum',
    depends_on_past=False,
    bash_command='wget --output-document ' + temporaldir + 'humidity.csv.zip https://github.com/manuparra/MaterialCC2020/raw/master/humidity.csv.zip',
    # bash_command='wget --output-document ' + temporaldir + 'humidity.csv.zip https://github.com/manuparra/MaterialCC2020/blob/master/humidity.csv.zip',
    dag=dag,
)

CapturaDatosTemp = BashOperator(
    task_id='CapturaDatosTemp',
    depends_on_past=False,
    bash_command='wget --output-document ' + temporaldir + 'temperature.csv.zip  https://github.com/manuparra/MaterialCC2020/raw/master/temperature.csv.zip',
    # bash_command='curl -o ' + temporaldir + 'temperature.csv.zip https://github.com/manuparra/MaterialCC2020/raw/master/temperature.csv.zip',
    dag=dag,

)


DescomprimirDatos=  BashOperator(
    task_id='Descomprimir',
    depends_on_past=False,
    bash_command='unzip -u ' + temporaldir + 'humidity.csv.zip -d ' + temporaldir + ' ; unzip -u ' + temporaldir + 'temperature.csv.zip -d ' + temporaldir,
    dag=dag
)     

SelectUnifDatos = PythonOperator(
    task_id='SelecUnifDatos',
    depends_on_past=True,
    python_callable=select_unific_dat,
    op_args=['' + temporaldir + 'temperature.csv','' + temporaldir + 'humidity.csv'],
    dag=dag,
)

UnitTest = BashOperator(
    task_id='TDD_unit_test',
    depends_on_past=False,
    bash_command='cd ' + cc_p2dir + ' && python3 test.py',
    dag=dag,
)

LanzarContenedor = BashOperator(
    task_id='LanzarContenedor',
    depends_on_past=False,
    # bash_command='docker rm -f prediccion_mongodb ; docker run -d -p 27017:27017 --name prediccion_mongodb mongo:latest',
    bash_command='cd ' + cc_p2dir + ' && docker-compose up --build -d && mongoimport --host localhost --db meteosat_db --collection predicciones --headerline --type csv --file ' + temporaldir + 'datos_san_francisco.csv',
    # bash_command='cd ' + cc_p2dir + ' && cp ' + temporaldir + 'datos_san_francisco.csv . && docker-compose up --build -d && docker-compose up --build -d',
    dag=dag
)

ClonarRepoGit = BashOperator(
    task_id='ClonarRepoGit',
    depends_on_past=False,
    bash_command='rm -r -f '+ cc_p2dir + ' ; git clone https://github.com/cr13/cc_p2.git -b microservicios ' + cc_p2dir ,
    dag=dag,
)

# LanzarContenedor 
# PrepararEntorno >> [CapturaDatosHum, CapturaDatosTemp]>> DescomprimirDatos >> SelectUnifDatos

[PrepararEntorno >> [CapturaDatosHum, CapturaDatosTemp] >> DescomprimirDatos >> [SelectUnifDatos, ClonarRepoGit] >> UnitTest]  >> LanzarContenedor

# PrepararEntorno.set_downstream([CapturaDatosHum,CapturaDatosTemp])
# DescomprimirDatos.set_upstream([CapturaDatosHum,CapturaDatosTemp])
# DescomprimirDatos.set_downstream([SelectUnifDatos, ClonarRepoGit])
# UnitTest.set_upstream([SelectUnifDatos, ClonarRepoGit])
# LanzarContenedor.set_upstream(UnitTest)
