

-- Active Flights  - tabla de hechos
-- despegues
DROP TABLE IF EXISTS tefmail_coderhouse.flights_dep;

CREATE TABLE tefmail_coderhouse.flights_dep (
	flight_date 				DATE, 
	departure_airport			VARCHAR(255),
	departure_timezone			VARCHAR(255), 
	departure_iata				VARCHAR(25), 
	departure_icao				VARCHAR(25),
	departure_terminal			VARCHAR(25),
	departure_gate				INT,
	departure_delay				INT,
	departure_scheduled			TIMESTAMP,
	departure_estimated			TIMESTAMP,
	departure_actual	 		TIMESTAMP,
	departure_estimated_runway	TIMESTAMP,
	departure_actual_runway		TIMESTAMP,
	arrival_airport				VARCHAR(255), 
	arrival_timezone			VARCHAR(255),
	arrival_iata				VARCHAR(25), 
	arrival_icao				VARCHAR(25),
	arrival_terminal			VARCHAR(25),
	arrival_gate				INT, 
	arrival_baggage				VARCHAR(25), 
	arrival_delay				INT,
	arrival_scheduled 			TIMESTAMP,
	arrival_estimated 			TIMESTAMP, 
	arrival_actual 				TIMESTAMP,
	arrival_estimated_runway	TIMESTAMP,
	arrival_actual_runway		TIMESTAMP,
	airline_name				VARCHAR(255),
	airline_iata				VARCHAR(25), 
	airline_icao				VARCHAR(25),
	flight_number				VARCHAR(25), 
	flight_iata					VARCHAR(25),
	flight_icao					VARCHAR(25) 
)

sortkey(departure_scheduled)
;

-- arribos
DROP TABLE IF EXISTS tefmail_coderhouse.flights_arr;

CREATE TABLE tefmail_coderhouse.flights_arr (
	flight_date 				DATE, 
	departure_airport			VARCHAR(255),
	departure_timezone			VARCHAR(255), 
	departure_iata				VARCHAR(25), 
	departure_icao				VARCHAR(25),
	departure_terminal			VARCHAR(25),
	departure_gate				INT,
	departure_delay				INT,
	departure_scheduled			TIMESTAMP,
	departure_estimated	  		TIMESTAMP,
	departure_actual	 		TIMESTAMP,
	departure_estimated_runway	TIMESTAMP,
	departure_actual_runway		TIMESTAMP,
	arrival_airport				VARCHAR(255), 
	arrival_timezone			VARCHAR(255),
	arrival_iata				VARCHAR(25), 
	arrival_icao				VARCHAR(25),
	arrival_terminal			VARCHAR(25),
	arrival_gate				INT, 
	arrival_baggage				VARCHAR(25), 
	arrival_delay				INT,
	arrival_scheduled 			TIMESTAMP,
	arrival_estimated 			TIMESTAMP, 
	arrival_actual 				TIMESTAMP,
	arrival_estimated_runway	TIMESTAMP,
	arrival_actual_runway		TIMESTAMP,
	airline_name				VARCHAR(255),
	airline_iata				VARCHAR(25), 
	airline_icao				VARCHAR(25),
	flight_number				VARCHAR(25), 
	flight_iata					VARCHAR(25),
	flight_icao					VARCHAR(25) 
)

sortkey(arrival_scheduled)
;

-- Airports - tabla de dimension

DROP TABLE IF EXISTS tefmail_coderhouse.airports;

CREATE TABLE tefmail_coderhouse.airports (
	airport_id      INT PRIMARY KEY,
	gmt             INT,
	iata_code       VARCHAR(25),
	city_iata_code  VARCHAR(25),
	icao_code       VARCHAR(25),
	country_iso2    VARCHAR(25),
	geoname_id      VARCHAR(25),
	latitude        REAL,
	longitude       REAL,
	airport_name    VARCHAR(255),
	country_name    VARCHAR(255),
	timezone        VARCHAR(255)
)

DISTSTYLE ALL
sortkey(iata_code)
;

-- Airlines - tabla de dimension

DROP TABLE IF EXISTS tefmail_coderhouse.airlines;

CREATE TABLE tefmail_coderhouse.airlines (

 	fleet_average_age       REAL,
 	airline_id              INT,
    callsign                VARCHAR(25),
    hub_code                VARCHAR(25),
    iata_code               VARCHAR(25),
    icao_code               VARCHAR(25),
    country_iso2            VARCHAR(25),
    date_founded            INT,
    iata_prefix_accounting  INT,
    airline_name            VARCHAR(255),
    country_name            VARCHAR(255),
    fleet_size              INT,
    status                  VARCHAR(25),
    type                    VARCHAR(255)
)

DISTSTYLE ALL
sortkey(iata_code)
;


