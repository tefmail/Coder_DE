# Importamos librerias
from utility import *
from datetime import date
import os

config_path =  "..\config\config.ini"
# ------------------------------------
# TABLAS DE HECHOS
# ------------------------------------
# Definimos parámetros de extracción, dividimos en arribos y partidas
def load_fact_table(config_path,ti):
    # CONEXION CON API
    # Genero la url con el endpoint = "flights"
    url = conn_api(config_path,"flights")
    params_flights_dep = {"dep_iata": "EZE"} 
    params_flights_arr = {"arr_iata": "EZE"} 

    print("Creando dataframe de partidas (departures)....")
    df_flights_dep  = fact_table_db(create_dataframe(url, params_flights_dep))
    print("Dataframe departures Ok!")

    print("Creando dataframe de arribos...")
    df_flights_arr  = fact_table_db(create_dataframe(url, params_flights_arr))
    print("Dataframe arribos Ok!")

    # ------------------------------------
    # Carga a la base de datos
    # ------------------------------------
    # Coneactamos a redshift y creamos el objeto de conexion
    conn = connect_to_db(config_path,"redshift")

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

    # Cuantos vuelos han partido retrasados desde Ezeiza el dia de hoy
    df_today_list = [date.today() for item in range(df_flights_dep["flight_date"].size)]
    df_today = pd.DataFrame()
    df_today["date"] = df_today_list
    df_today["date"] = pd.to_datetime(df_today["date"])

    df_aux = pd.concat([df_today["date"].reset_index(drop=True), df_flights_dep[["airline_name","departure_delay", "flight_date"]].reset_index(drop=True)],ignore_index = True, axis=1)
    df_aux.columns =["date", "airline_name","departure_delay", "flight_date"]

    departure_delays = df_aux[(df_aux["departure_delay"]>180) & (df_aux["date"] == df_aux["flight_date"]) ]["departure_delay"].count()
    print(f"vuelos con demora: {departure_delays}")
    print(type(departure_delays))
    ti.xcom_push(key='delay', value=departure_delays)

    return

# ------------------------------------
# TABLAS DE DIMENSION
# ------------------------------------
# Cargo dimensiones el primer dia del mes
# Defino la url del correspondiente endpoint y cargo el datafame
def load_dim_tables(config_path):

    today = date.today()
    if today.day == 1:
        mje_final = "Actualizando dimensiones"
    # CONEXION CON API
    # Genero la url con el endpoint = "airport"

        # Airports
        url_airports = conn_api(config_path,"airports")

        #total_pag_airpot = requests.get(url_airlines).json()["pagination"]["total"]

        print("Creando dataframe de aeropuertos (airports)....")
        params = {}
        params['offset'] = 0
        df_airports = create_dataframe(url_airports, params)

        print("Dataframe airports Ok!")

        # Airlines data
        url_airlines = conn_api(config_path,"airlines")
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

    # Coneactamos a redshift y creamos el objeto de conexion
    conn = connect_to_db(config_path,"redshift")
    # Cargo tablas de dimension del "esquema" que simulamos ser staging
    if today.day == 1:
        params['offset'] = 0
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
    else:
        mje_final ="Las dimensiones se actualizan el 1ro de cada mes"

    return print(mje_final)


if __name__ == "__main__":
    load_fact_table(config_path)
    load_dim_tables(config_path)
