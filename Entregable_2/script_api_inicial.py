# Importamos librerias
from utility import *

# CONEXION CON API
# Genero la url con el endpoint = "flights"
url = conn_api("flights")

# TABLAS DE HECHOS
# Definimos parámetros de extracción, dividimos en arribos y partidas
params_flights_dep = {"dep_iata": "EZE"} 
params_flights_arr = {"arr_iata": "EZE"} 

print("Creando dataframe de partidas (departures)....")
df_flights_dep  = create_dataframe(url, params_flights_dep)
print("Dataframe departures Ok!")
print("Creando dataframe de arribos...")
df_flights_arr  = create_dataframe(url, params_flights_arr)
print("Dataframe arribos Ok!")

# Selecciono columnas de interes y cambio tipos de dato 
def table_db(df):
    for df in flights:
        df=df[['flight_date','departure.airport','departure.timezone','departure.iata','departure.icao','departure.terminal','departure.gate','departure.delay','departure.scheduled','departure.estimated','departure.actual','departure.estimated_runway','departure.actual_runway','arrival.airport','arrival.timezone','arrival.iata','arrival.icao','arrival.terminal','arrival.gate','arrival.baggage','arrival.delay','arrival.scheduled','arrival.estimated','arrival.actual','arrival.estimated_runway','arrival.actual_runway','airline.name','airline.iata','airline.icao','flight.number','flight.iata','flight.icao']]
        df.columns=['flight_date','departure_airport','departure_timezone','departure_iata','departure_icao','departure_terminal','departure_gate','departure_delay','departure_scheduled','departure_estimated','departure_actual','departure_estimated_runway','departure_actual_runway','arrival_airport','arrival_timezone','arrival_iata','arrival_icao','arrival_terminal','arrival_gate','arrival_baggage','arrival_delay','arrival_scheduled','arrival_estimated','arrival_actual','arrival_estimated_runway','arrival_actual_runway','airline_name','airline_iata','airline_icao','flight_number','flight_iata','flight_icao']

        # TIPO DE DATOS
    
        time_variables = ['flight_date', 'departure_scheduled', 'departure_estimated', 'departure_actual', 'departure_estimated_runway','departure_actual_runway', 'arrival_scheduled', 'arrival_estimated', 'arrival_actual', 'arrival_estimated_runway', 'arrival_actual_runway']
        for i in time_variables:
            df[i] = pd.to_datetime(df[i])

        # lleno con 0 para cambiar el tipo de dato
        df[["departure_delay","arrival_delay"]] = df[["departure_delay","arrival_delay"]].fillna(0)
        df[["departure_delay","arrival_delay"]] = df[["departure_delay","arrival_delay"]].astype('int32')

        cat_variables = ['departure_airport','departure_timezone','departure_iata','departure_icao','departure_terminal','departure_gate','arrival_airport','arrival_timezone','arrival_iata','arrival_icao','arrival_terminal','arrival_gate','arrival_baggage','airline_name','airline_iata','airline_icao','flight_number','flight_iata','flight_icao']
        for j in cat_variables:
            df[cat_variables] = df[cat_variables].astype("category")

    return df

df_flights_dep = table_db(df_flights_dep)
df_flights_arr = table_db(df_flights_arr)

# TABLAS DE DIMENSION
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

# Cargo tablas de dimension
dims = [df_airports, df_airlines]
tbl_dims_names = ["airports", "airlines"]

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