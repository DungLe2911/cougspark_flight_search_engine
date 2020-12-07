#!/usr/bin/env python
# coding: utf-8

# Sequential Algorithm
# Original query problem:  Which country (or) territory has the highest number of Airports
# Modified the original problem
# Listed the top k countries based on the highest number of Airports
# Where K is used as user input

import os
import time
import pandas as pd
import numpy as np
from pandasql import sqldf
pysqldf = lambda q: sqldf(q, globals())
path = "../datasets/cleanedv2"
#path = "../datasets/enriched"

class highestNoAirports:
    
    def __init__(self, k):
        self.k = k
        
    def highestNairports(self,X,Y):
        query = """SELECT country, COUNT('airport_id') AS airport_count
        FROM airports
        LEFT JOIN countries ON airports.country = countries.name 
        GROUP BY country
        ORDER BY airport_count DESC;"""
        result = pysqldf(query).head(k)
        return result
    
if __name__ == "__main__":
    
    k = int(input ("Please enter number of countries you want to see: "))
    start = time.time()
    airports = pd.read_csv(os.path.join(path,'airports.csv'))
    countries = pd.read_csv(os.path.join(path,'countries.csv'))
    ha=highestNoAirports(k)
    result=ha.highestNairports(airports, countries)
    end = time.time()
    print(result)
    print('\nDuration of Sequential Algorithm:{}'.format(end-start))






