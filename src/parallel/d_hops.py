#!/usr/bin/env python
# coding: utf-8

# In[ ]:


sc


# In[ ]:


sc.stop()


# In[54]:


from pyspark import SparkContext, SparkConf
import time
import os


# In[55]:


from graphframes import *
from pyspark.sql import SQLContext, SparkSession, DataFrame


# In[56]:


from pyspark import SparkContext, SparkFiles

#spark = SparkSession.builder.appName("Niki-job").master('spark://ip-172-31-59-32.us-west-2.compute.internal:7077').getOrCreate()
spark = SparkSession.builder.appName("Niki-job").master('local').getOrCreate() # local version
path = "../datasets/enriched"


# In[57]:


class d_hops:
    def __init__(self, city, d):
        self.city = city
        self.d = d
    def run(self):
        spark.sparkContext.addFile(os.path.join(path, 'airports.csv'))
        spark.sparkContext.addFile(os.path.join(path, 'routes.csv'))
        
        self.airports = spark.read.options(header=True, inferSchema=True, delimiter=',').csv(SparkFiles.get("airports.csv"))
        self.routes = spark.read.options(header=True, inferSchema=True, delimiter=',').csv(SparkFiles.get("routes.csv"))
        
        self.generateGraph()
        
        self.CitiesReachableByD()
    
    def generateGraph(self):    
        g_airports = self.airports.withColumnRenamed("airport_id", "id")
        g_routes = self.routes.withColumnRenamed("src_id", "src")
        g_routes = g_routes.withColumnRenamed("dst_id", "dst")
        
        v = g_airports
        e = g_routes
        
        self.graph = GraphFrame(v, e)
        
    def getAirportsInCity(self):
        self.airports.createOrReplaceTempView("airports")
        self.routes.createOrReplaceTempView("routes")
    
        s = "\"" + self.city +"\""
        # filter edges containing airport IDs present in the city
        q = spark.sql(""" select airport_id from airports where city = %s """  % s)
        self.airportsInCity = [(row['airport_id']) for row in q.collect()] # convert result to list
        
    def CitiesReachableByD(self):
        self.getAirportsInCity()
        cities = []
        
        if (self.d == 1):
            paths = "(x)-[e]->(y)"
            for airport in self.airportsInCity:
                trips = self.graph.find(paths).filter("x.id == "+str(airport)).filter("y.id !="+str(airport)).limit(10)
                cities.extend([row['city'] for row in trips.select("y.city", "y.country").dropDuplicates().collect()])
                
        elif (self.d == 2):
            paths = "(x)-[e]->(b); (b)-[e2]->(y)"
            for airport in self.airportsInCity:
                trips = self.graph.find(paths).filter("x.id == "+str(airport)).filter("y.id !="+str(airport)).limit(10)
                cities.extend([row['city'] for row in trips.select("y.city", "y.country").dropDuplicates().collect()])
                
        else: # do 3 hops by default
            self.d = 3
            paths = "(x)-[e]->(b); (b)-[e2]->(c) ; (c)-[e3]->(y)"
            for airport in self.airportsInCity:
                trips = self.graph.find(paths).filter("x.id == "+str(airport)).filter("y.id !="+str(airport)).limit(10)
                cities.extend([row['city'] for row in trips.select("y.city", "y.country").dropDuplicates().collect()])

        self.cities = cities
        self.printCities()
        return self.cities
    
    def printCities(self):
        print("\nCities reachable from {} in {} hops (limit 10):".format(self.city, self.d))
        for c in self.cities:
            print(c)


# In[58]:


if __name__ == "__main__":
    c = input("Enter a city name: ")
    while True:
        d = int(input("Enter a number of hops: "))
        if (d > 0):
            break
    start_time = time.time()
    dh = d_hops(c, d)
    res = dh.run()
    end_time = time.time()
    print("\nparallel d hops took {} s".format(end_time-start_time))


# In[60]:


spark.stop()


# In[ ]:




