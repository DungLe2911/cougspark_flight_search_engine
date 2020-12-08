# %%
#Parallel Algorithm
#Original query problem:  Find list of airports operating in the Country X

# Modified the original problem
# Listed the airports by taking user input

# %%
sc

# %%
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkFiles
import time

spark = SparkSession.builder.appName("Richard-job").master('local[*]').getOrCreate()
# spark = SparkSession.builder.appName("Richard-job").master('spark://ip-172-31-59-32.us-west-2.compute.internal:7077').getOrCreate()

# %%
path = "../datasets/enriched"
# path = "../datasets/cleanedv2"
airportFile = "airports.csv"
airportPath= os.path.join(path,airportFile)

# %%
class airportsInCountry:
    def __init__(self, k):
        path = "../datasets/enriched"
        airportFile = "airports.csv"
        self.country = k
        spark.sparkContext.addFile(os.path.join(path,airportFile))
        self.airports = spark.read.options(header=True, inferSchema=True, delimiter=',').csv(SparkFiles.get("airports.csv"))
        self.airports.createOrReplaceTempView("airports")
              
    def airportsInX(self):
        q  =""" 
            SELECT name, country, IATA, ICAO, timezone, DST
            FROM airports
            WHERE country = "{}"
            """.format(k)
        res = spark.sql(q)
        res.select('name','country','IATA','timezone','DST').show()

# %%
if __name__ == "__main__":
    k = input("Enter a Country name for airport list lookup:: ")
    ac=airportsInCountry(k)
    start = time.time()
    ac.airportsInX()
    end = time.time()
    print("Parallel time: {}".format(end-start))

# %%
spark.stop()

# %%
sc.stop()