# Importamos librerias
from utility import *
from datetime import date
import os



###############################################
#---------------------------------------------

config_path =  "..\config\config.ini"

# Coneactamos a redshift y creamos el objeto de conexion
conn = connect_to_db(config_path,"redshift")


query = """
SELECT COUNT(DISTINCT flight_number) FROM flights_dep
WHERE FLIGHT_DATE =  CURRENT_DATE 
AND DEPARTURE_DELAY  > 180
"""

data = pd.read_sql(query, conn) 

delay = str(data.values.tolist()).strip("[]")
print(delay)