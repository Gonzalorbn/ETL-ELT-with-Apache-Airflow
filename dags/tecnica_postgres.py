#script que define el flujo de trabajo de Airflow con 4 tareas secuenciales
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, date
from operators.PostgresFileOperator import PostgresFileOperator
from airflow.operators.bash import BashOperator


DATE=str(date.today()).replace('-','')

with DAG(
    dag_id="tecnica_postgres",
    start_date= datetime(2023,9,28),

) as dag:
    #En esta tarea1, se crea la tabla en postgresql
    task_1 = PostgresOperator(
        task_id="Crear_tabla",
        postgres_conn_id = "postgres_localhost",
        sql= """
            create table if not exists tecnica_ml (
                id varchar(100),
                site_id varchar(100),
                title varchar(100),
                price varchar(100),
                sold_quantity varchar(100),
                thumbnail varchar(100),
                created_date varchar(8),
                primary key(id,created_date)
            )    
        """
    )
    #se ejecuta el script externo encargado de consultar la API
    task_2 = BashOperator(
        task_id = "Consulting_API",
        bash_command = "python /opt/airflow/plugins/tmp/consult_api.py"
    )
    # Crea un operador personalizado (clase PostgresFileOperator) para insertar nuevos datos.
    # La tarea 'Insertar_Data' utiliza la operación 'write', que ejecuta la función write_in_db
    # para insertar los datos del archivo file.tsv en la tabla 'tecnica_ml'.
    task_3 = PostgresFileOperator(
        task_id = "Insertar_Data",
        operation="write",
        config={"table_name":"tecnica_ml"}
    )
    #Se utiliza nuevamente nuestro operador personalizado para leer de la base de datos y realizar una consulta sql,
    #tambien tomamos la desición de que si la consulta arroja mismos ID de ayer, no es necesario enviar email.
    #la operacion "read" va a ejecutar la funcion "read from db" encargada de lo dicho.
    task_4 = PostgresFileOperator(
        task_id = "Reading_Data",
        operation="read",
        config={"query": f"SELECT * from tecnica_ml WHERE sold_quantity != 'null' and created_date = '{DATE}' and cast(price as decimal) * cast(sold_quantity as int) > 7000000"}
    )
    #dependencias de tareas del DAG
    task_1 >> task_2 >> task_3 >> task_4