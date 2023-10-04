import requests
from io import BytesIO
import MySQLdb
import datetime
import zipfile
import xml.etree.ElementTree as ET
import pandas as pd
import pandasql as psql
from sqlalchemy import create_engine

# Nombre del archivo de configuración
config_file = "C:\\Users\\lcano\\Desktop\\etl_ventas\\config.txt"
# Nombre del archivo de configuración
ruta_directorio = 'C:\\Users\\lcano\\Desktop\\Excel_impresoras' 

 # Obtener la fecha y hora actual
fecha_main = datetime.datetime.now()
# Restar un día
un_dia = datetime.timedelta(days=1)
fecha_diccionario = fecha_main - un_dia
fecha_diccionario = fecha_diccionario.date()

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

def descargar_descomprimir_xml_zip(ip, zeta):
    try:
        # URL del archivo .zip que deseas descargar
        zip_url = f'http://user:9999@{ip}/jornada.zip?DESDE={zeta}&HASTA={zeta}'

        # Realizar la solicitud para descargar el archivo .zip
        response = requests.get(zip_url)

        # Verificar si la solicitud fue exitosa
        if response.status_code == 200:
            # Crear un objeto BytesIO para almacenar el contenido del archivo .zip
            zip_content = BytesIO(response.content)
            
            # Descomprimir el contenido del archivo .zip
            with zipfile.ZipFile(zip_content) as zip_ref:
                # Almacenar los contenidos del archivo ZIP en una variable
                zip_contents = {}
                for file_name in zip_ref.namelist():
                    with zip_ref.open(file_name) as file:
                        zip_contents[file_name] = file.read()

                # Acceder al contenido de un archivo específico en la variable
                first_file_name = list(zip_contents.keys())[0]
                first_file_content = zip_contents.get(first_file_name)
                
                if first_file_content:
                    decoded_content = first_file_content.decode('utf-8')  # Ajusta la codificación si es diferente
                    return decoded_content
                else:
                    print(f"El archivo '{first_file_name}' no se encuentra en el ZIP.")
        else:
            print(f"La solicitud falló con el código de estado: {response.status_code}")

    except Exception as e:
        print(f"Otro Error: {str(e)}")

def obtener_datos_desde_xml(decoded_content):
    try:
        # Cargar el archivo XML
        tree = ET.ElementTree(ET.fromstring(decoded_content))
        root = tree.getroot()

        # Crear una lista para almacenar los datos del XML
        data_list = []

        # Recorrer los elementos del XML y extraer la información
        documento_elements = root.findall(".//Documento")

        for documento_element in documento_elements:
    
            apertura_element = documento_element.find(".//Apertura")
            punto_venta = apertura_element.find("POS").text
            fecha = apertura_element.find("Fecha").text
            hora = apertura_element.find("Hora").text
            numero_documento = apertura_element.find("NumeroDocumento").text
    
            venta_element = documento_element.find(".//Venta")
            try:
                descripcion = venta_element.find("Descripcion").text
            except AttributeError:
                descripcion = "Descripcion no encontrada"
            venta_element = documento_element.find(".//Totales")
            try:
                precio = venta_element.find("Final").text
            except AttributeError:
                precio = "Precio no encontrado"

            # Agregar más campos aquí según tu estructura XML
            data_list.append({
                'PUNTO_VENTA': punto_venta,
                'FECHA_VENTA': fecha,
                'HORA_VENTA': hora,
                'NRO_TICKET': numero_documento,
                'MONTO_VENTA': precio,
                'CLASE': descripcion
                })

        df = pd.DataFrame(data_list)
        return df

    except Exception as e:
        print(f"Error al procesar el archivo XML: {str(e)}")
        return None
    

def guardar_dataframe_como_xlsx(df, ip, zeta, fecha_formateada, ruta_directorio):
    try:

        ruta_archivo = f'{ruta_directorio}\\{ip}_{zeta}_{fecha_formateada}.xlsx'
        df.to_excel(ruta_archivo, index=False)
        print(f"DataFrame guardado exitosamente en: {ruta_archivo}")
    except Exception as e:
        print(f"Error al guardar el DataFrame como XLSX: {str(e)}")

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

print(fecha_diccionario)
print(type(fecha_diccionario))
# Convertir la fecha a una cadena con un formato específico
fecha_formateada = fecha_diccionario.strftime("%Y-%m-%d")


# Crear un cursor para ejecutar consultas
db.connect()


#--where fecha= '{fecha_formateada}'
# Define tu consulta SELECT
query_inicio = f"""SELECT distinct ip,zeta FROM ip_gco 
                    where fecha = '{fecha_formateada}'
                    and ip in ("10.6.70.1", "10.6.70.2", "10.6.70.3", "10.6.70.4", "10.6.70.5", "10.6.70.6", "10.6.70.7", "10.6.70.8",
"10.7.70.1", "10.7.70.2", "10.7.70.3", "10.7.70.4", "10.7.70.5", "10.7.70.6")   
"""
print(query_inicio)
# Ejecutar la sentencia SQL
resultado = db.execute_query(query_inicio)
print(resultado)
# Inicializar un diccionario para almacenar los resultados
resultado_lista = []


# Recorre los resultados
for row in resultado:
    ip, zeta = row
    resultado_lista.append((ip, zeta))

# Cerrar la conexión
db.close()

# Recorre los resultados
print(resultado_lista)
#resultado_dict = {'10.7.70.4': 2036}
# Control de hora del proceso total
hora_inicio = datetime.datetime.now()

for ip, zeta in resultado_lista:
    # Control de hora del proceso de una ip
    hora_inicio_individual = datetime.datetime.now()
    # Control de hora del proceso
    hora_inicio = datetime.datetime.now()
    # Obtén la fecha y hora actual
    hora_archivo = datetime.datetime.now()
    # Crea un objeto timedelta con un día de diferencia
    un_dia = datetime.timedelta(days=1)
    # Resta un día a la fecha y hora actual
    hora_archivo = hora_archivo - un_dia
    # Formatear la fecha en el formato deseado
    formato_personalizado = "%Y%m%d"
    fecha_formateada = hora_archivo.strftime(formato_personalizado)
    
    # ip del ciclo divido con split por el .
    partes_ip = ip.split(".")
    # obtengo la VIA
    estacion = partes_ip[1]
    #obtengo la Estacion
    via = partes_ip[-1]
    
    try:
        #funcion obtener y descomprimir XML
        contenido_xml = descargar_descomprimir_xml_zip(ip, zeta)
        
        df = obtener_datos_desde_xml(contenido_xml)
    
        # Convertir la columna fecha_apertura a formato de fecha en el DataFrame
        df['FECHA_VENTA'] = pd.to_datetime(df['FECHA_VENTA'], format='%y%m%d')
        #df['fecha_apertura'] = df['fecha_apertura'].dt.strftime('%d%m%y')
        # Establecer una fecha base
        df['HORA_VENTA'] = pd.to_datetime(df['HORA_VENTA'], format='%H%M%S').dt.time 

        # Funcion guardar dataframe en computadora local como respaldo
        guardar_dataframe_como_xlsx(df, ip, zeta, fecha_formateada, ruta_directorio)

        # Definicion de query que se realiza sobre el Dataframe, si queres modificar algo de la query se hace aca
        query = f"""
                SELECT  
                       {via} as VIA
                       ,{estacion} as ESTACION
                       ,FECHA_VENTA
                       ,HORA_VENTA
                       ,SUBSTR(PUNTO_VENTA, LENGTH(PUNTO_VENTA) - LENGTH(LTRIM(PUNTO_VENTA, '0')) + 1) as PUNTO_VENTA
                       ,MONTO_VENTA
                       ,NRO_TICKET  AS NRO_TICKET
                       ,SUBSTR(CLASE, -1) as CLASE
                   
                   
                FROM df 
                where CLASE != 'Descripcion no encontrada'
            """
        # Se ejecuta la query definida y se almacena en la variable result
        result = psql.sqldf(query, locals())

        # Establecer la conexión a la base de datos MySQL
        db.connect()
    
        # Itera a través de las filas del DataFrame y ejecuta consultas INSERT
        for index, row in result.iterrows():
            tiempo_insertar = datetime.datetime.now()
            query = """
                INSERT INTO ventas_efectivo (ESTACION, VIA, FECHA_VENTA, HORA_VENTA, PUNTO_VENTA, MONTO_VENTA, NRO_TICKET, CLASE)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s) 
            """

            values = (
                row['ESTACION'],
                row['VIA'],
                row['FECHA_VENTA'],
                row['HORA_VENTA'],
                row['PUNTO_VENTA'],
                row['MONTO_VENTA'],
                row['NRO_TICKET'],
                row['CLASE']
                )
            try:   
                db.execute_query(query, values)
            except Exception as e:
                print(f"Error al insertar: {e}")
        finalizo_insertar = datetime.datetime.now()
        tiempo_tardo_insertar = finalizo_insertar - tiempo_insertar 
        print(F"ESTE CICLO TARDO {tiempo_tardo_insertar}")
        # Realiza la confirmación y cierra la conexión
        db.commit()
        db.close()
        # Control de hora de finalizacion de proceso
        hora_final_inividual = datetime.datetime.now()
        # Resta de hora de Inicio y Final
        duracion_individual = hora_final_inividual - hora_inicio_individual
        print(f"Se inserto la ip: {ip} y tardo: {duracion_individual}")
    except Exception as e:
        print(f"Error al conectarse la ip: {ip}, la misma se guardara en la tabla ip_error. ERROR {e}")
       
             
print("Inserción completada.")
# Control de hora de finalizacion de proceso
hora_final = datetime.datetime.now()
# Resta de hora de Inicio y Final
duracion = hora_final - hora_inicio
print(f"El proceso duro: {duracion}")