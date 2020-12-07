#!/usr/bin/env python
# coding: utf-8

# In[3]:


sc


# In[2]:


sc.stop()


# In[18]:


import time
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkFiles

#spark = SparkSession.builder.appName("Niki-job").master('spark://ip-172-31-59-32.us-west-2.compute.internal:7077').getOrCreate()
spark = SparkSession.builder.appName("Niki-job").master('local').getOrCreate() # local version
path = "../datasets/enriched"


# In[19]:


class pXstops:
    def __init__(self, X):
        self.X = X
        
    def run(self):
        spark.sparkContext.addFile(os.path.join(path, 'routes.csv'))
        routes = spark.read.options(header=True, inferSchema=True, delimiter=',').csv(SparkFiles.get("routes.csv"))
        self.routes = routes
        self.routes.createOrReplaceTempView("routes")
        
        q = spark.sql(""" 
            select distinct airline_name 
            from routes where stops = '{}'
            order by airline_name ASC
            limit 10
            """.format(self.X))
        
        self.result = [(row['airline_name']) for row in q.collect()] # convert to list, store in result
        self.printOutput()
        return self.result
        
    def printOutput(self):
        print("\nresult (limit 10): ")
        for a in self.result:
            print(a)


# In[20]:


if __name__ == "__main__":
    while True:
        x = int(input("Find out which airlines have X stops! Enter a value for X: "))
        if (x >= 0):
            break
    start_time = time.time()
    x_stops = pXstops(x)
    res = x_stops.run()
    end_time = time.time()
    print("\nparallel Xstops took {} s".format(end_time-start_time))


# In[21]:


spark.stop()


# In[ ]:




