from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
from email.message import EmailMessage
import ssl
import smtplib
import os

class PostgresFileOperator(BaseOperator):
    @apply_defaults
    def __init__(self, operation, config={}, *args, **kwargs):
        super(PostgresFileOperator, self).__init__(*args, **kwargs)
        self.operation = operation
        self.config = config
        self.postgres_hook = PostgresHook(postgres_conn_id='postgres_localhost') #atributo para establecer conexion a postgresql
    
    def execute(self, context):#toma la desición de escribir o leer llamando a la funcion write_in_db
        if self.operation == "write":
            self.write_in_db()
        elif self.operation == "read":
            self.read_from_db()
    def write_in_db(self):
        # Eliminar todos los registros existentes,caso contrario no deja agregar información y tira un error..
        delete_query = "DELETE FROM tecnica_ml"
        self.postgres_hook.run(delete_query)

        # Cargar nuevos datos contenidos en file.tsv
        self.postgres_hook.bulk_load(self.config.get('table_name'), 'plugins/tmp/file.tsv')

    def read_from_db(self):
        # read from db with a SQL query
        conn = self.postgres_hook.get_conn() #variable que contiene un objeto de conexion a la base de datos
        cursor = conn.cursor()  #le asigno el objeto cursor para realizar consultas
        cursor.execute(self.config.get("query"))
        
        # doc toma el valor de una fila en cada ciclo for, y cada fila (osea doc) se guarda en cada posicion de la lista "data"
        data = [doc for doc in cursor]
        print("LEYENDO DE BASE DE DATOS Y DECIDIENDO SI ENVIAR CORREO:")
        print(data)

        inspect_path = '/opt/airflow/plugins/operators/inspect.tsv'
        flag = False  # Por defecto, no se envía el correo

        # Verificar si el archivo existe y no está vacío, esto es para no tener que enviar la
        # misma información que ya envié ayer y no tiene sentido enviarla de nuevo
        if os.path.exists(inspect_path) and os.path.getsize(inspect_path) > 0:
            existing_ids = set() #defino una coleccion de identificadores unicos 
            with open(inspect_path, 'r', encoding='utf-8') as existing_file:
                for line in existing_file: #recorro cada linea del archivo
                    _id, *_ = line.strip().split('\t') 
                    existing_ids.add(_id) 

            # Verificar si hay al menos un _id diferente
            new_ids = set(item[0] for item in data)
            if not new_ids.issubset(existing_ids): #si new_ids no es un subconjunto de existing_ids enviar email
                flag = True
        else:
            # Si el archivo no existe o está vacío, enviar correo
            flag = True

        #pase lo que pase debo copiar el contenido nuevo igual en inspect, para que al dia siguiente se compruebe y ocupe el rol de
        #datos viejos y hacer la comparación
        with open('/opt/airflow/plugins/operators/inspect.tsv', 'w', encoding='utf-8') as file:
            for item in data:
                _id, site_id, title, price, sold_quantity, thumbnail, created_date = item
                file.write(f'{_id}\t{site_id}\t{title}\t{price}\t{sold_quantity}\t{thumbnail}\t{created_date}\n')

        # si hay resultados del query, enviar email
        if data and flag:
            email_sender = 'completar con email del que envía'
            email_password = 'añadir un App password que debe crear el sender (o añadir esta variable con la pass desde airflow)'
            email_receiver = 'email del destinatario'

            subject = 'Alerta!! producto con demasiadas ventas'
 
            body = f"""
            <html>
                <body>
                    <p>Hemos detectado un producto que está explotando en ventas:</p>
                    <ul>
                        {''.join(f'<li>{str(item)}</li>' for item in data)}
                    </ul>
                </body>
            </html>
            """

            em = EmailMessage()
            em['From'] = email_sender
            em['To'] = email_receiver
            em['Subject'] = subject
            em.add_alternative(body, subtype='html')  

            context = ssl.create_default_context()

            with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
                smtp.login(email_sender, email_password)
                smtp.sendmail(email_sender, email_receiver, em.as_string())
