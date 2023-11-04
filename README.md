# Coder_DE REEDME

## _Entregable 1_

Este trabajo tiene como objetivo registrar los vuelos que arriban y despegan de Ezeiza, aeropuerto internacional de Buenos Aires.

En princio se generan las siguientes tablas:
- flights
- airports
- airlines

## Features

- Importamos informacion de la API [Aviationstack](https://aviationstack.com/) de vuelos que parten (departure) y arriban (arrivals) del aeropuerto Ezeiza
- Creamos tablas para los vuelos regitrados
- Importamos información en tablas de dimensiones, de las aerolineas y también de información de los aeropuertos, para el caso en que mas adelante se quiera utilizar esta información.
- Creamos las tablas de dimension para aeorlineas y aeropuertos

Se eligió para las tablas de hechos, **flights_dep** y **flights_arr**, el ordenamiento por fecha de partida y de arribo respectivamente para ordenar temporalmente los vuelos y establecer una linea temporal de actividad del aeropuerto. 

Para las tablas de dimensión se eligió como sort key, el campo iata_code, pensando en que pueda ser usual una union (join) de tablas mediante este campo y las tablas de hechos.

Todos los codigo utilizados se encuentran en el repositorio GitHub [Coder_DE](https://github.com/tefmail/Coder_DE.git).
 
