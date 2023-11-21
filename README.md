# Coder_DE REEDME

## _Entregable 1_

Este trabajo tiene como objetivo registrar los vuelos que arriban y despegan de Ezeiza, aeropuerto internacional de Buenos Aires.

En princio se generan las siguientes tablas:
- flights_arr (arribos)
- flights_dep (partidas)
- airports (aeropuertos)
- airlines (aerolineas)

## Features

- Importamos informacion de la API [Aviationstack](https://aviationstack.com/) de vuelos que parten (departure) y arriban (arrivals) del aeropuerto Ezeiza
- Creamos una tabla para cada tipo de hecho, arrivals y departures, debido a una limitación en la cantidad de registros que permite el plan gratuito de la api. 
- Importamos información en tablas de dimensiones, de las aerolineas y también de información de los aeropuertos, para el caso en que mas adelante se quiera utilizar esta información.
- Creamos las tablas de dimension para aeorlineas y aeropuertos con información actualizada, con una instancia previa de staging que luego se depura para cargar la informción actualizada en las tablas de dimension finales.

Se eligió para las tablas de hechos, **flights_dep** y **flights_arr**, el ordenamiento por fecha de partida y de arribo respectivamente para ordenar temporalmente los vuelos y establecer una linea temporal de actividad del aeropuerto. 

Para las tablas de dimensión se eligió como sort key, el campo iata_code, pensando en que pueda ser usual una union (join) de tablas mediante este campo y las tablas de hechos. Se muestra a continuacion la creacion de las tablas de dimension

### Airports
>CREATE TABLE tefmail_coderhouse.airports (
>	airport_id INT PRIMARY KEY,
>	gmt VARCHAR(25),
>	iata_code VARCHAR(25),
>	city_iata_code VARCHAR(25),
>	icao_code VARCHAR(25),
>	country_iso2 VARCHAR(25),
>	geoname_id VARCHAR(25),
>	latitude REAL,
>	longitude REAL,
>	airport_name VARCHAR(255),
>	country_name VARCHAR(255),
>	timezone VARCHAR(255)
>) DISTSTYLE ALL sortkey(iata_code);

### Airlines 
>CREATE TABLE tefmail_coderhouse.airlines (
>	fleet_average_age REAL,
>	airline_id INT,
>	callsign VARCHAR(25),
>	hub_code VARCHAR(25),
>	iata_code VARCHAR(25),
>	icao_code VARCHAR(25),
>	country_iso2 VARCHAR(25),
>	date_founded INT,
>	iata_prefix_accounting INT,
>	airline_name VARCHAR(255),
>	country_name VARCHAR(255),
>	fleet_size INT,
>	status VARCHAR(25),
>	type VARCHAR(255)
>) DISTSTYLE ALL sortkey(iata_code);


Todos los códigos utilizados se encuentran en el repositorio GitHub [Coder_DE](https://github.com/tefmail/Coder_DE.git), ordenados por Entregas, de acuerdo al avance que fue teniendo el proyecto.