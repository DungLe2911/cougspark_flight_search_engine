#!/usr/bin/env python
# coding: utf-8

# In[24]:


import pandas as pd
from pandasql import sqldf
import time


# In[25]:


class Xstops:
    def __init__(self, X):   
        self.X = X
        
    def run(self):
        routes = pd.read_csv('../datasets/enriched/routes.csv')
        
        q = """ 
            select distinct airline_name 
            from routes where stops = '{}'
            order by airline_name ASC
            limit 10
            """.format(self.X)
        
        self.result = list(sqldf(q)['airline_name'])
        self.printResult()
        return self.result

    def printResult(self):
        print("\nresult (limit 10): ")
        for a in self.result:
            print(a)


# In[26]:


if __name__ == "__main__":
    while True:
        x = int(input("Find out which airlines have X stops! Enter a value for X: "))
        if (x >= 0):
            break
    start_time = time.time()
    x_stops = Xstops(x)
    res = x_stops.run()
    x_stops.printResult()
    end_time = time.time()
    print("\nsequential Xstops took {} s".format(end_time-start_time))


# In[ ]:




