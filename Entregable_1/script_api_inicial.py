import requests
import json
import pandas as pd
import sqlalchemy as sa

import os
from configparser import ConfigParser

from pathlib import Path

config = ConfigParser()

# configuramos la direccion del archivo de credeciales
config_dir = "Entregable_1\config\config.ini"
config.read(config_dir)

access_key = config["api_aviation"]["access_key"] 

# Armamos la url con el endpoint y especificando las credenciales como parámetros
url_base = "http://api.aviationstack.com/v1"
endpoint_1 = "flights"
params_flights = {"dep_iata": "EZE", "offset": 100}

url_1 = f"{url_base}/{endpoint_1}?access_key={access_key}"

# Obtenemos datos haciendo un GET
# usando el método get de la librería
flights = requests.get(url_1, params = params_flights)
json_data_flights = flights.json()["data"]

# Generamos el data frame de los datos de vuelos (flights)
df_flights = pd.json_normalize(json_data_flights)

# Generamos el data frame para la dimension de aeropuertos.
# Endpoint y url
endpoint_2 = "airports"
url_2 = f"{url_base}/{endpoint_2}?access_key={access_key}"
# Obtenemos los datos con Get
airports = requests.get(url_2)
json_data_airports = airports.json()["data"]
# Generamos el dataframe
df_airports = pd.json_normalize(json_data_airports)

# Generamos el data frame para la dimension de airlines.
# Endpoint y url
endpoint_3 = "airlines"
url_3 = f"{url_base}/{endpoint_3}?access_key={access_key}"
# Obtenemos los datos con Get
airlines = requests.get(url_3)
json_data_airlines = airlines.json()["data"]
# Generamos el dataframe
df_airlines = pd.json_normalize(json_data_airlines)
 