# Coder_DE REEDME

Este trabajo tiene como objetivo registrar los vuelos que arriban y despegan de Ezeiza, aeropuerto internacional de Buenos Aires.

## Definicion Tablas

Se generan tablas de hechos(vuelos) y algunas de tablas de dimensiones(aeropuertos y aerolineas). Los nombres de tablas son las siguientes:
- flights_arr (arribos)
- flights_dep (partidas)
- airports (aeropuertos)
- airlines (aerolineas)

En el trabajo se realizó una primera carga de las tablas de dimensionn la base de datos de redshift provista por CoderHouse, y se genero un código de descarga de la API, transformacion de datos y carga de la información en las tablas de hechos. Se crearon funciones para que el código sea mas limpio. Antes de cargar las tablas en la base de datos, se realizo el trabajo de transformacion de datos, para ello se cambiaron los tipos de datos y se seleccionaron algunas columnas de interés respecto a la totalidad provista por la API. Se seleccionó la columna de fecha de arribo o salida, según corresponda para el ordenamiento de las tablas, siendo la temporalidad de los vuelos un factor importante para los análisis que puedan querer realizarse y por lo tanto óptimo para el funcionamiento de las query. 

Se generó un DAG que permite la carga de las tablas de hechos de forma diaria, mientras que la carga y/o actualizacion de las tablas de dimensión es mensual, pensando en dimensiones lentamente cambiantes.

Asi mismo se configuró una tarea que envía un correo electrónico cuando la tarea ha fallado y se genero la alerta cuando existen vuelos que hayan salido demorados en mas de 3hs desde el aeropuerto.

## Features

- Importamos informacion de la API [Aviationstack](https://aviationstack.com/) de vuelos que parten (departure) y arriban (arrivals) del aeropuerto Ezeiza
- Creamos una tabla para cada tipo de hecho, arrivals y departures, debido a una limitación en la cantidad de registros que permite el plan gratuito de la api. 
- Importamos información en tablas de dimensiones, de las aerolineas y también de información de los aeropuertos, para el caso en que mas adelante se quiera utilizar esta información.
- Creamos las tablas de dimension para aeorlineas y aeropuertos con información actualizada, con una instancia previa de staging que luego se depura para cargar la informción actualizada en las tablas de dimension finales.
- Se asignan tipos de datos correcto y se seleccionaron columnas de interes en las tablas de hecho. 
- Se cargan las tablas a la base de datos.

Se eligió para las tablas de hechos, **flights_dep** y **flights_arr**, el ordenamiento por fecha de partida y de arribo respectivamente para ordenar temporalmente los vuelos y establecer una linea temporal de actividad del aeropuerto. 

Para las tablas de dimensión se eligió como sort key, el campo iata_code, pensando en que pueda ser usual una union (join) de tablas mediante este campo y las tablas de hechos. Se muestra a continuacion la creacion de las tablas de dimension

### Airports
```sh
CREATE TABLE tefmail_coderhouse.airports ( 
	airport_id INT PRIMARY KEY, 
	gmt VARCHAR(25), 
	iata_code VARCHAR(25), 
	city_iata_code VARCHAR(25), 
	icao_code VARCHAR(25), 
	country_iso2 VARCHAR(25), 
	geoname_id VARCHAR(25), 
	latitude REAL, 
	longitude REAL, 
	airport_name VARCHAR(255), 
	country_name VARCHAR(255), 
	timezone VARCHAR(255) 
) DISTSTYLE ALL sortkey(iata_code); 
```

### Airlines 
```sh
CREATE TABLE tefmail_coderhouse.airlines (
	fleet_average_age REAL,
	airline_id INT,
	callsign VARCHAR(25),
	hub_code VARCHAR(25),
	iata_code VARCHAR(25),
	icao_code VARCHAR(25),
	country_iso2 VARCHAR(25),
	date_founded INT,
	iata_prefix_accounting INT,
	airline_name VARCHAR(255),
	country_name VARCHAR(255),
	fleet_size INT,
	status VARCHAR(25),
	type VARCHAR(255)
) DISTSTYLE ALL sortkey(iata_code);
```

### Departure Flights
```sh
CREATE TABLE IF NOT EXISTS tefmail_coderhouse.flights_dep (
	flight_date DATE,
	departure_airport VARCHAR(255),
	departure_timezone VARCHAR(255),
	departure_iata VARCHAR(25),
	departure_icao VARCHAR(25),
	departure_terminal VARCHAR(25),
	departure_gate INT,
	departure_delay INT,
	departure_scheduled TIMESTAMP,
	departure_estimated TIMESTAMP,
	departure_actual TIMESTAMP,
	departure_estimated_runway TIMESTAMP,
	departure_actual_runway TIMESTAMP,
	arrival_airport VARCHAR(255),
	arrival_timezone VARCHAR(255),
	arrival_iata VARCHAR(25),
	arrival_icao VARCHAR(25),
	arrival_terminal VARCHAR(25),
	arrival_gate INT,
	arrival_baggage VARCHAR(25),
	arrival_delay INT,
	arrival_scheduled TIMESTAMP,
	arrival_estimated TIMESTAMP,
	arrival_actual TIMESTAMP,
	arrival_estimated_runway TIMESTAMP,
	arrival_actual_runway TIMESTAMP,
	airline_name VARCHAR(255),
	airline_iata VARCHAR(25),
	airline_icao VARCHAR(25),
	flight_number VARCHAR(25),
	flight_iata VARCHAR(25),
	flight_icao VARCHAR(25)
) sortkey(departure_scheduled);
```

### Arribal Flights
```sh
CREATE TABLE IF NOT EXISTS tefmail_coderhouse.flights_arr (
	flight_date DATE,
	departure_airport VARCHAR(255),
	departure_timezone VARCHAR(255),
	departure_iata VARCHAR(25),
	departure_icao VARCHAR(25),
	departure_terminal VARCHAR(25),
	departure_gate INT,
	departure_delay INT,
	departure_scheduled TIMESTAMP,
	departure_estimated TIMESTAMP,
	departure_actual TIMESTAMP,
	departure_estimated_runway TIMESTAMP,
	departure_actual_runway TIMESTAMP,
	arrival_airport VARCHAR(255),
	arrival_timezone VARCHAR(255),
	arrival_iata VARCHAR(25),
	arrival_icao VARCHAR(25),
	arrival_terminal VARCHAR(25),
	arrival_gate INT,
	arrival_baggage VARCHAR(25),
	arrival_delay INT,
	arrival_scheduled TIMESTAMP,
	arrival_estimated TIMESTAMP,
	arrival_actual TIMESTAMP,
	arrival_estimated_runway TIMESTAMP,
	arrival_actual_runway TIMESTAMP,
	airline_name VARCHAR(255),
	airline_iata VARCHAR(25),
	airline_icao VARCHAR(25),
	flight_number VARCHAR(25),
	flight_iata VARCHAR(25),
	flight_icao VARCHAR(25)
) sortkey(arrival_scheduled);
```