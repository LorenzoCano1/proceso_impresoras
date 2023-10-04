import requests
from bs4 import BeautifulSoup
import MySQLdb
import datetime
from requests.exceptions import ConnectionError, ConnectTimeout, ReadTimeout

vias = ["10.1.70.1", "10.1.70.2", "10.1.70.3", "10.1.70.4",
"10.2.70.1", "10.2.70.2", "10.2.70.3", "10.2.70.4",
"10.3.70.1","10.3.70.2", "10.3.70.3", "10.3.70.4", "10.3.70.5",
"10.4.70.1", "10.4.70.2", "10.4.70.3", "10.4.70.4", "10.4.70.5",
"10.5.70.1", "10.5.70.2", "10.5.70.3", "10.5.70.4", "10.5.70.5", "10.5.70.6", "10.5.70.7", "10.5.70.8", "10.5.70.10",
"10.6.70.1", "10.6.70.2", "10.6.70.3", "10.6.70.4", "10.6.70.5", "10.6.70.6", "10.6.70.7", "10.6.70.8",
"10.7.70.1", "10.7.70.2", "10.7.70.3", "10.7.70.4", "10.7.70.5", "10.7.70.6",
"10.8.70.1", "10.8.70.2", "10.8.70.3", "10.8.70.4", "10.8.70.5", "10.8.70.6", "10.8.70.7", "10.8.70.8", "10.8.70.9", "10.8.70.10",
"10.8.70.11", "10.8.70.12", "10.8.70.13", "10.8.70.14", "10.8.70.15", "10.8.70.16", "10.8.70.17", "10.8.70.18", "10.8.70.19", "10.8.70.20",
"10.8.70.21", "10.8.70.22", "10.8.70.23", "10.8.70.24", "10.8.70.25", "10.8.70.26", "10.8.70.27", "10.8.70.28", "10.8.70.29", "10.8.70.30",
"10.8.70.31", "10.8.70.32", "10.8.70.41", "10.8.70.42", "10.8.70.43", "10.8.70.44", "10.8.70.45", 
"10.9.70.1", "10.9.70.2", "10.9.70.3", "10.9.70.4",
"10.10.70.1", "10.10.70.2", "10.10.70.3", "10.10.70.4",
"10.11.70.1", "10.11.70.2",
"10.12.70.1", "10.12.70.2", "10.12.70.3", "10.12.70.4", "10.12.70.5", "10.12.70.6",
"10.13.70.1", "10.13.70.2", "10.13.70.3", "10.13.70.4",
"10.14.70.1", "10.14.70.2", "10.14.70.3", "10.14.70.4",
"10.15.70.1", "10.15.70.2", "10.15.70.3", "10.15.70.4",
"10.16.70.1", "10.16.70.2", "10.16.70.3", "10.16.70.4",
"10.17.70.1", "10.17.70.2", "10.17.70.3", "10.17.70.4",
"10.18.70.1", "10.18.70.2", "10.18.70.3", "10.18.70.4", "10.18.70.5", "10.18.70.6", "10.18.70.7", "10.18.70.8", "10.18.70.9", "10.18.70.10", 
"10.18.70.11", "10.18.70.12", "10.18.70.13", "10.18.70.14", "10.18.70.15", "10.18.70.16", "10.18.70.17", "10.18.70.18"]



# Crear una lista para almacenar los valores cuando se produce una excepción
ip_error = []

class MySQLDatabase:
    def __init__(self, host, user, password, database, charset='latin1'):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.charset = charset
        self.connection = None

    def connect(self):
        try:
            self.connection = MySQLdb.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                charset=self.charset
            )
            print("Conexión exitosa a la base de datos.")
        except MySQLdb.Error as e:
            print("Error al conectar a la base de datos:", e)

    def execute_query(self, query, values=None):
        if self.connection:
            try:
                cursor = self.connection.cursor()
                if values:
                    cursor.execute(query, values)
                else:
                    cursor.execute(query)
                result = cursor.fetchall()
                cursor.close()
                return result
            except MySQLdb.Error as e:
                print("Error al ejecutar la consulta:", e)
        else:
            print("No hay una conexión a la base de datos.")

    def commit(self):
        if self.connection:
            try:
                self.connection.commit()
                print("Cambios guardados en la base de datos.")
            except MySQLdb.Error as e:
                self.connection.rollback()
                print("Error al guardar cambios en la base de datos:", e)

    def close(self):
        if self.connection:
            self.connection.close()
            print("Conexión cerrada.")
        else:
            print("No hay una conexión activa para cerrar.")

# Nombre del archivo de configuración
config_file = "C:\\Users\\lcano\\Desktop\\etl_ventas\\config.txt"

# Leer los datos desde el archivo de configuración
config = {}
with open(config_file, "r") as file:
    for line in file:
        key, value = line.strip().split(": ")
        config[key] = value


# Uso de la clase en un Jupyter Notebook
db = MySQLDatabase(
    host=config.get("Host"),
    user=config.get("User"),
    password=config.get("Password"),
    database=config.get("Database"),
    charset='latin1'
)


db.connect()


for ip in vias:
    

    url = f'http://user:9999@{ip}/audit.html'
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'
    }

    # Realizar la solicitud GET y obtener el contenido HTML
    try:
        response = requests.get(url, headers=headers, timeout=150)
        html = response.content

        # Crear un objeto BeautifulSoup
        soup = BeautifulSoup(html, 'html.parser')

        input_element = soup.find('input', {'name': 'DATAFORMATZETA'})

        # Obtener el valor del atributo 'value' del elemento <input>
        valor = int(input_element['value']) - 1
        
        # Obtener la fecha y hora actual
        fecha_hora = datetime.datetime.now()
        # Restar un día
        un_dia = datetime.timedelta(days=1)
        fecha_hora = fecha_hora - un_dia
        fecha_hora = fecha_hora.date()
        
        # Sentencia SQL para la inserción
        sentencia_sql = "INSERT INTO ip_gco (ip, zeta, fecha) VALUES (%s, %s, %s)"

        # Verificar que la conexión a la base de datos esté abierta
        if db.connection is None:
            db.connect()

        print(f"Valor: {valor} de la ip: {ip}")
        
        # Valores a insertar
        valores = (ip, valor, fecha_hora)

        # Ejecutar la sentencia SQL
        db.execute_query(sentencia_sql, valores)
        db.commit()

    except ConnectionError as e:
        if isinstance(e, ConnectTimeout):
            # Manejar el error de tiempo de conexión (ConnectTimeout)
            print(f"Error ConnectTimeout a la ip: {ip}, el error es: {type(e).__name__}")
        elif isinstance(e, ReadTimeout):
            # Manejar el error de tiempo de lectura (ReadTimeout)
            print(f"Error ReadTimeout a la ip: {ip}, el error es: {type(e).__name__}")
            ip_error.append(ip)
        else:
            # Otros errores de conexión
            print(f"Otro error a la ip: {ip}, el error es: {type(e).__name__}")
    except Exception as e:
        # Manejar otras excepciones (si es necesario)
        print(f"Otro Error de la ip: {ip} tipo de excepcion {type(e).__name__}")
    
        
# Cerrar la conexión
db.close()