#!/usr/bin/env python
# coding: utf-8
# Parallel Algorithm

# Original query problem:  Which country (or) territory has the highest number of Airports
# Modified the original problem
# Listed the top k countries based on the highest number of Airports
# Where K is used as user input

import time
import os
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkFiles

spark = SparkSession.builder.appName("cpts415-bigdata").master('spark://ip-172-31-59-32.us-west-2.compute.internal:7077').getOrCreate()
# spark = SparkSession.builder.appName("Nur-job").master('local[*]').getOrCreate()
# path = "../datasets/cleanedv2"
path = "../datasets/enriched"


class highestNoAirports:
    def __init__(self, k):
        self.k = k 
        spark.sparkContext.addFile(os.path.join(path, 'airports.csv'))
        spark.sparkContext.addFile(os.path.join(path, 'countries.csv'))
        airports = spark.read.options(header=True, inferSchema=True, delimiter=',').csv(SparkFiles.get("airports.csv"))
        countries = spark.read.options(header=True, inferSchema=True, delimiter=',').csv(SparkFiles.get("countries.csv"))
        self.airports = airports.createOrReplaceTempView("airports")
        self.countries = countries.createOrReplaceTempView("countries")

    def highestNairports(self):
        query = """SELECT country, count('airport_id') as airport_count 
                   FROM airports 
                   LEFT JOIN countries ON airports.country = countries.name   
                   GROUP BY country
                   ORDER BY airport_count DESC
                   """
        result = spark.sql(query)
        result.select('country','airport_count').show(self.k)
    
if __name__ == "__main__":
    k = int(input ("Please enter number of countries you want to see: "))
    start = time.time()
    ha=highestNoAirports(k)
    ha.highestNairports()
    end = time.time()
    print('\nDuration of Parallel Algorithm:{}'.format(end-start))



