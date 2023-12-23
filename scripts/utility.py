import requests
import json
import numpy as np
import pandas as pd
import sqlalchemy as sa
import os
from configparser import ConfigParser
from pathlib import Path
import smtplib
from airflow.models import Variable

# Conexion a la API
def conn_api(config_path,endpoint):
    """
    Construye la cadena de conexión a la API aviationstack
    a partir de un archivo de configuración.
    """
    # Lee el archivo de configuración
    #config_path =  Path(os.path.dirname(str(os.getcwd())) + "\config\config.ini")  
    print(config_path)
    parser = ConfigParser()
    parser.read(config_path)
   
    # Lee la sección de configuración de PostgreSQL
    config = parser["api_aviation"]
    pwd = config['access_key']
    url_base = "http://api.aviationstack.com/v1"
    
    # Construye la cadena de conexión
    conn_string = f"{url_base}/{endpoint}?access_key={pwd}"

    return conn_string

def create_dataframe(url,params = '1'):
    """
    Creo los dataframe a partir del endpoint definido y los parametros, si los hay.
    """
    if params == '1':
        request = requests.get(url)
        json_data = request.json()["data"]
        df  = pd.json_normalize(json_data)
    else:
        request = requests.get(url, params)
        json_pag = request.json()["pagination"]
        json_data = request.json()["data"]
        df  = pd.json_normalize(json_data)
        i = 0
        while i < json_pag['total']:
            i = i + 100
            params['offset'] = i
            request = requests.get(url, params)
            json_data = request.json()["data"]
            df = pd.concat([df,pd.json_normalize(json_data)])
    
    return df


def connect_to_db(config_path,config_section):
    """
    Construye la cadena de conexión a la base de datos
    a partir de un archivo de configuración.
    """

    # Lee el archivo de configuración
    #config_path =  Path(os.path.dirname(str(os.getcwd())) + "\config\config.ini")
    parser = ConfigParser()
    parser.read(config_path)

    # Lee la sección de configuración de PostgreSQL
    config = parser[config_section]
    host = config['host']
    port = config['port']
    dbname = config['dbname']
    schema = config["schema"]
    username = config['username']
    pwd = config['pwd']

    # Construye la cadena de conexión
    conn_string = f'postgresql://{username}:{pwd}@{host}:{port}/{dbname}'
    
    # Crea una conexión a la base de datos.
    engine = sa.create_engine(
        conn_string,
        connect_args = {"options" : f"-csearch_path={schema}" }
        )
    conn = engine.connect()

    return conn



# Selecciono columnas de interes y cambio tipos de dato 
def fact_table_db(df):
    columns_names = ['flight_date','departure.airport','departure.timezone','departure.iata','departure.icao','departure.terminal','departure.gate','departure.delay','departure.scheduled','departure.estimated','departure.actual','departure.estimated_runway','departure.actual_runway','arrival.airport','arrival.timezone','arrival.iata','arrival.icao','arrival.terminal','arrival.gate','arrival.baggage','arrival.delay','arrival.scheduled','arrival.estimated','arrival.actual','arrival.estimated_runway','arrival.actual_runway','airline.name','airline.iata','airline.icao','flight.number','flight.iata','flight.icao']

    dg = df[columns_names]
    
    dg.columns = ['flight_date','departure_airport','departure_timezone','departure_iata','departure_icao','departure_terminal','departure_gate','departure_delay','departure_scheduled','departure_estimated','departure_actual','departure_estimated_runway','departure_actual_runway','arrival_airport','arrival_timezone','arrival_iata','arrival_icao','arrival_terminal','arrival_gate','arrival_baggage','arrival_delay','arrival_scheduled','arrival_estimated','arrival_actual','arrival_estimated_runway','arrival_actual_runway','airline_name','airline_iata','airline_icao','flight_number','flight_iata','flight_icao']

        # TIPO DE DATOS
    
    time_variables = ['flight_date', 'departure_scheduled', 'departure_estimated', 'departure_actual', 'departure_estimated_runway','departure_actual_runway', 'arrival_scheduled', 'arrival_estimated', 'arrival_actual', 'arrival_estimated_runway', 'arrival_actual_runway']

    for i in time_variables:
        dg.loc[:,i] = pd.to_datetime(dg.loc[:,i])
        

    # lleno con 0 para cambiar el tipo de dato
    int_variables = ["departure_delay","arrival_delay"]
    for i in int_variables:
        dg.loc[:,i] = dg.loc[:,i].fillna(0)
        dg.loc[:,i] = dg.loc[:,i].astype('int32')

    cat_variables = ['departure_airport','departure_timezone','departure_iata','departure_icao','departure_terminal','departure_gate','arrival_airport','arrival_timezone','arrival_iata','arrival_icao','arrival_terminal','arrival_gate','arrival_baggage','airline_name','airline_iata','airline_icao','flight_number','flight_iata','flight_icao']

    for j in cat_variables:
        dg.loc[:,cat_variables] = dg.loc[:,cat_variables].astype("category")

    return dg

def enviar(**context):
    try:
        x = smtplib.SMTP('smtp.gmail.com',587)
        x.starttls()
        
        print(f"Mi clave es: {Variable.get('gmail_secret')}")
        x.login(
            'tefmail@gmail.com',
            Variable.get('gmail_secret')
        )

        subject = f'Airflow reporte {context["dag"]}  {context["ts"]}'
        body_text = f'DAG Ejecutado: {context["task_instance_key_str"]} con éxito.'
        message='Subject: {}\n\n{}'.format(subject,body_text).encode('utf-8')
        
        x.sendmail('tefmail@gmail.com', 'tefmail@gmail.com', message)
        print('Exito')
    except Exception as exception:

    
        print(exception)
        print('Failure')

