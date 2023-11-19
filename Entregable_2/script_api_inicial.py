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
df_flights_dep  = fact_table_db(create_dataframe(url, params_flights_dep))
print("Dataframe departures Ok!")

print("Creando dataframe de arribos...")
df_flights_arr  = fact_table_db(create_dataframe(url, params_flights_arr))
print("Dataframe arribos Ok!")

# ------------------------------------
# TABLAS DE DIMENSION
# ------------------------------------
# Defino la url del correspondiente endpoint y cargo el datafame

# Airports
url_airports = conn_api("airports")

#total_pag_airpot = requests.get(url_airlines).json()["pagination"]["total"]

print("Creando dataframe de aeropuertos (airports)....")
params = {}
params['offset'] = 0
df_airports = create_dataframe(url_airports, params)

print("Dataframe airports Ok!")

# Airlines data
url_airlines = conn_api("airlines")
print("Creando dataframe de aerolineas (airlines)....")
df_airlines = create_dataframe(url_airlines, params)

print("Dataframe airlines Ok!")

# cabmiamos tipos de variables
airports_convert_dict = { 'airport_id': 'int32' , 'gmt': 'category','iata_code': "category", 'city_iata_code': "category", 'icao_code': "category", 'country_iso2': "category",'geoname_id': "category", 'latitude': "float", 'longitude': "float", 'airport_name': "category", 'country_name': "category", 'timezone': "category"}

airlines_convert_dict = {'fleet_average_age': "float", 	'airline_id': "int32", 'callsign': "category", 'hub_code': "category", 'iata_code': "category", 'icao_code': "category", 'country_iso2': "category", 'date_founded': "int32", 'iata_prefix_accounting': "int32", 'airline_name': "category", 'country_name': "category", 'fleet_size': "int32", 'status': "category", 'type': "category" }

dicts = [airports_convert_dict, airlines_convert_dict]
for j in dicts:
    for i in airlines_convert_dict.keys():
        if airlines_convert_dict[i] == "int32":
            df_airlines[i].fillna(0, inplace = True)

df_airports = df_airports.astype(airports_convert_dict)

df_airlines = df_airlines.astype(airlines_convert_dict)

print("Dataframe dimensions Ok!")

# ------------------------------------
# Carga a la base de datos
# ------------------------------------
# Coneactamos a redshift y creamos el objeto de conexion
conn = connect_to_db("redshift")

print("Cargando tabla de hechos...")
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

print("Tablas de hechos cargadas")

# Cargo tablas de dimension del "esquema" que simulamos ser staging
print("Cargando dimensiones...en stage")
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

print("Actualizo dimension airports...")
# Actualizacion airports
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

print("airports actualizada")

print("Actualizo dimension airlines...")
# Actualizacion airlines
query_airlines = """
BEGIN;
MERGE INTO airlines
USING stage_airlines AS source
ON airlines.iata_code = source.iata_code
WHEN MATCHED THEN
UPDATE
SET
	fleet_average_age = source.fleet_average_age,
	airline_id = source.airline_id,
	callsign = source.callsign,
	hub_code = source.hub_code,
	iata_code = source.iata_code,
	icao_code = source.icao_code,
	country_iso2 = source.country_iso2,
	date_founded = source.date_founded,
	iata_prefix_accounting = source.iata_prefix_accounting,
	airline_name = source.airline_name,
	country_name = source.country_name,
	fleet_size = source.fleet_size,
	status = source.status,
	type = source.type
WHEN NOT MATCHED THEN
INSERT (
    fleet_average_age,
    airline_id,
    callsign,
    hub_code,
    iata_code,
    icao_code,
    country_iso2,
    date_founded,
    iata_prefix_accounting,
    airline_name,
    country_name,
    fleet_size,
    status,
    type
)
VALUES (
    source.fleet_average_age,
    source.airline_id,
    source.callsign,
    source.hub_code,
    source.iata_code,
    source.icao_code,
    source.country_iso2,
    source.date_founded,
    source.iata_prefix_accounting,
    source.airline_name,
    source.country_name,
    source.fleet_size,
    source.status,
    source.type
);
COMMIT;
"""

conn.execute(query_airlines)

print("airports actualizada")