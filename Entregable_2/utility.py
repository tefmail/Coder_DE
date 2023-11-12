

import requests
import json
import pandas as pd
import sqlalchemy as sa
import os
from configparser import ConfigParser
from pathlib import Path


# Conexion a la API
def conn_api(endpoint):
    """
    Construye la cadena de conexión a la API aviationstack
    a partir de un archivo de configuración.
    """
    # Lee el archivo de configuración
    config_path =  Path(str(os.getcwd()) + "\config\config.ini")  
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
            df = df.append(pd.json_normalize(json_data))
    
    return df


def connect_to_db(config_section):
    """
    Construye la cadena de conexión a la base de datos
    a partir de un archivo de configuración.
    """

    # Lee el archivo de configuración
    config_path =  Path(str(os.getcwd()) + "\config\config.ini")
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


