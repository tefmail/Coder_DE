# Importamos librerias
from utility import *

# CONEXION CON API
# Genero la url con el endpoint = "flights"
url = conn_api("flights")

# ------------------------------------
# TABLAS DE HECHOS
# ------------------------------------
# Definimos parámetros de extracción, dividimos en arribos y partidas
params_flights_dep = {"dep_iata": "EZE"} 
params_flights_arr = {"arr_iata": "EZE"} 

print("Creando dataframe de partidas (departures)....")
df_flights_dep  = table_db(create_dataframe(url, params_flights_dep))
print("Dataframe departures Ok!")

print("Creando dataframe de arribos...")
df_flights_arr  = table_db(create_dataframe(url, params_flights_arr))
print("Dataframe arribos Ok!")

# ------------------------------------
# TABLAS DE DIMENSION
# ------------------------------------
# Defino la url del correspondiente endpoint y cargo el datafame

# Airports
url_airports = conn_api("airports")
print("Creando dataframe de aeropuertos (airports)....")
df_airports = create_dataframe(url_airports)
print("Dataframe airports Ok!")

# Airlines data
url_airlines = conn_api("airlines")
print("Creando dataframe de aerolineas (airlines)....")
df_airlines = create_dataframe(url_airlines)
print("Dataframe airlines Ok!")

# ------------------------------------
# Carga a la base de datos
# ------------------------------------
# Coneactamos a redshift y creamos el objeto de conexion
conn = connect_to_db("redshift")

# Cargo tablas de hechos
flights = [df_flights_dep, df_flights_arr]
tbl_fact_names = ["flights_dep", "flights_arr"]

for df, tbl_name in zip(flights, tbl_fact_names):
    df.to_sql(
        name = tbl_name,
        con = conn,
        if_exists = "append",
        schema = "tefmail_coderhouse",
        method = "multi",
        index = False
    )

# Cargo tablas de dimension del "esquema" que simulamos ser staging
dims = [df_airports, df_airlines]
tbl_dims_names = ["stage_airports", "stage_airlines"]

for df, tbl_name in zip(dims, tbl_dims_names):
    df.to_sql(
        name = tbl_name,
        con = conn,
        if_exists = "replace",
        schema = "tefmail_coderhouse",
        method = "multi",
        index = False
    )
# FALTA IMPLEMENTAR LA INSERCION DE LAS TABLAS DE DIMENSION, UTILIZANDO EL MERGE JUNTO CON LAS OPERACIONES UPDATE E INSERT PARA NO IMPORTAR DATOS DUPLICADOS.


query = """
BEGIN;
MERGE INTO airports
USING stage_airports AS source
ON airports.iata_code = source.iata_code
WHEN MATCHED THEN
UPDATE
SET
    airport_id = source.airport_id,
	gmt = source.gmt,
	city_iata_code = source.city_iata_code,
	icao_code = source.icao_code,
	country_iso2 = source.country_iso2,
	geoname_id = source.geoname_id,
	latitude = source.latitude,
	longitude = source.longitude,
	airport_name = source.airport_name,
	country_name = source.country_name,
	timezone = source.timezone
WHEN NOT MATCHED THEN
INSERT (
    airport_id,
	gmt,
    iata_code,
	city_iata_code,
	icao_code,
	country_iso2,
	geoname_id,
	latitude,
	longitude,
	airport_name,
	country_name,
	timezone
)
VALUES (
    source.airport_id,
    source.gmt,
    source.iata_code,
    source.city_iata_code,
    source.icao_code,
    source.country_iso2,
    source.geoname_id,
    source.latitude,
    source.longitude,
    source.airport_name,
    source.country_name,
    source.timezone
);
COMMIT;
"""

conn.execute(query)