# Importamos librerias
import requests
import json
import pandas as pd
import sqlalchemy as sa

import os
from configparser import ConfigParser

from pathlib import Path


# Conexion a la API
def conn_api(config_path, config_section, url_base, endpoint):
    # Lee el archivo de configuración
    parser = ConfigParser()
    parser.read(config_path)

    # Lee la sección de configuración de PostgreSQL
    config = parser[config_section]
    pwd = config['access_key']

    # Construye la cadena de conexión
    conn_string = f"{url_base}/{endpoint}?access_key={pwd}"

    return conn_string

def create_dataframe(url,params = '1'):
    if params == '1':
        request = requests.get(url)
        json_data = request.json()["data"]
        df  = pd.json_normalize(json_data)
    else:
        request = requests.get(url, params)
        json_data = request.json()["data"]
        df  = pd.json_normalize(json_data)
    
    return df

# Definimos parámetros de configuración de la API    
url_base = "http://api.aviationstack.com/v1"
config_dir =  Path(str(os.getcwd()) + "\config\config.ini") # print(os.getcwd()) + "\config\config.ini"
section = "api_aviation"

# Extracción datos endpoint = "flights"
url = conn_api(config_dir, section ,url_base, "flights")

# configuramos parámetros de extracción
params_flights_dep = {"dep_iata": "EZE"} 
params_flights_arr = {"arr_iata": "EZE"} 

df_flights_dep  = create_dataframe(url, params_flights_dep)
df_flights_arr  = create_dataframe(url, params_flights_arr)

df_airports = create_dataframe(conn_api(config_dir, section ,url_base, "airports"))
df_airlines = create_dataframe(conn_api(config_dir, section ,url_base, "airlines"))

