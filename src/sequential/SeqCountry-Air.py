# %%
import pandas as pd
import time
import numpy as np
import os
from pandasql import sqldf
pysqldf = lambda q: sqldf(q, globals())
path = "../datasets/enriched"

# %%
class CountryAirport:
    
    def __init__(self, target, airport):
        self.target = target
        self.airport = airport
        
    def AirportList(self):
        query = """SELECT *  FROM airport WHERE airport.country='{}'""".format(str(self.target))
        result = pysqldf(query)
        return result

# %%
if __name__ == "__main__":
    #target = input("Enter a Country name for airport list lookup: ")
    target = "Italy"
    start = time.time()
    airport = pd.read_csv(os.path.join(path,'airports.csv'))
    CA = CountryAirport(target, airport)
    result = CA.AirportList()
    result = result.drop(columns=['altitude','ICAO','IATA', 'longitude', 'latitude','TZ','country'])
    result.columns = ["Airport_id",'Name','City','Timezone','DST']
    end = time.time()
    display(result)
    print('\nSequential time for{}:{}'.format(end-start))

# %%
