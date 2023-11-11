# Importamos librerias
import requests
import json
import pandas as pd
import sqlalchemy as sa

import os
from configparser import ConfigParser

from pathlib import Path


# Conexion a la API
def conn_api(config_path, endpoint):
    # Lee el archivo de configuración
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

# Definimos parámetros de configuración de la API    

config_dir =  Path(str(os.getcwd()) + "\config\config.ini") 


# Extracción datos endpoint = "flights"
url = conn_api(config_dir, "flights")

# configuramos parámetros de extracción
params_flights_dep = {"dep_iata": "EZE"} 
params_flights_arr = {"arr_iata": "EZE"} 

print("inicio creacion de dataframe....")
df_flights_dep  = create_dataframe(url, params_flights_dep)
print("dataframe departures creado...")
print("continuo con arribos")
df_flights_arr  = create_dataframe(url, params_flights_arr)
print("dataframe arribos creado...")

# tablas de dimension
# Airports
url_airports = conn_api(config_dir, "airports")
df_airports = create_dataframe(url_airports)

print("dataframe airports creado...")

# Airlines data
url_airlines = conn_api(config_dir, "airlines")
df_airlines = create_dataframe(url_airlines)
print("dataframe airlines creado...finish")
# ------------------------------------
