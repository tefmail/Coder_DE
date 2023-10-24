import requests
import json
import pandas as pd

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
endpoint = "flights"

url = f"{url_base}/{endpoint}?access_key={access_key}"

# Obtenemos datos haciendo un GET
# usando el método get de la librería
resp = requests.get(url)

#estoy probando hacer cambios
print(resp.json()["data"])