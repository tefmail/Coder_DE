# Importamos librerias
from utility import *

# CONEXION CON API
# Genero la url con el endpoint = "flights"
url = conn_api("flights")

# Definimos parámetros de extracción, dividimos en arribos y partidas
params_flights_dep = {"dep_iata": "EZE"} 
params_flights_arr = {"arr_iata": "EZE"} 
"""
print("Creando dataframe de partidas (departures)....")
df_flights_dep  = create_dataframe(url, params_flights_dep)
print("Dataframe departures creado...")
print("Creando dataframe de arribos...")
df_flights_arr  = create_dataframe(url, params_flights_arr)
print("Dataframe arribos creado...")
"""
# TABLAS DE DIMENSION
# Defino la url del correspondiente endpoint y cargo el datafame

# Airports
url_airports = conn_api("airports")
print("Creando dataframe de aeropuertos (airports)....")
df_airports = create_dataframe(url_airports)
print("Dataframe airports creado...")

# Airlines data
url_airlines = conn_api("airlines")
print("Creando dataframe de aerolineas (airlines)....")
df_airlines = create_dataframe(url_airlines)

print("Dataframe airlines creado...finish")
# ------------------------------------
# Coneactamos a redshift y creamos el objeto de conexion
conn = connect_to_db("redshift")


# Cargar los datasets a la base de datos
# dfs = [df_flights_dep, df_flights_arr, df_airports, url_airlines]
# tbl_names = ["flights_dep", "flights_arr", "airports", "airlines"]
dfs = [df_airports, url_airlines]
tbl_names = ["airports", "airlines"]

for df, tbl_name in zip(dfs, tbl_names):
    df.to_sql(
        name = tbl_name,
        con = conn,
        if_exists = "replace",
        method = "multi",
        index = False,
    )