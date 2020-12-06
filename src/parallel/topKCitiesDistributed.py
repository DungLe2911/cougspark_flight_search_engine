#!/usr/bin/env python
# coding: utf-8

# Developer: Pinak Bhalerao
# 
# Email id: pinak.bhalerao@wsu.edu

# In[1]:


import sys
path="../../work/Pinak/"
sys.path.insert(0,path)
import dataModel
import time


# In[11]:


class topBusyAirports:
    
    def __init__(self, k=3):
        self.k= k
        self.g= dataModel.graph
    
    def extractAirportId(self, lst):
        return(i[0] for i in lst)
    
    def extractDegree(self,lst):
        return(i[1] for i in lst)
    
    def getBusyIncomingAirports(self):
        inDegreesDf= self.g.inDegrees
        inAp= inDegreesDf.orderBy(dataModel.col("inDegree").desc()).take(self.k)
        table= inDegreesDf.orderBy(dataModel.col("inDegree").desc())
        airportIds= list(self.extractAirportId(inAp))
        degrees= list(self.extractDegree(inAp))
        
        ########################
        print("airportIds: {}".format(airportIds))
        print("degrees: {}".format(degrees))
        ########################
        
        airportDf= dataModel.airports
        
        w = dataModel.Window().orderBy(dataModel.lit('A'))
        returnDf= table.join(airportDf, table.id == airportDf.airport_id, 'inner').drop(table.id)
        newDF= returnDf.orderBy(dataModel.col("inDegree").desc())
        return newDF
        
    def getBusyOutgoingAirports(self) -> dataModel.DataFrame:
        outDegreesDf= self.g.outDegrees
        outAp= outDegreesDf.orderBy(dataModel.col("outDegree").desc()).take(self.k)
        table= outDegreesDf.orderBy(dataModel.col("outDegree").desc())
        airportIds= list(self.extractAirportId(outAp))
        degrees= list(self.extractDegree(outAp))
        
        ########################
        print("airportIds: {}".format(airportIds))
        print("degrees: {}".format(degrees))
        ########################        
        
        airportDf= dataModel.airports
        w = dataModel.Window().orderBy(dataModel.lit('A'))
        returnDf= table.join(airportDf, table.id == airportDf.airport_id, 'inner').drop(table.id)
        newDF= returnDf.orderBy(dataModel.col("outDegree").desc())
        return newDF
    
    def inComing(self):
        result= self.getBusyIncomingAirports()
        result.select('inDegree','airport_id','city', 'country').show(self.k)
        #result= result.select('inDegree','airport_id','city', 'country').groupBy('city').sum('inDegree')
        #result.orderBy(dataModel.col("sum(inDegree)").desc()).show(self.k)
    
    def outGoing(self):
        result= self.getBusyOutgoingAirports()
        result.select('outDegree','airport_id','city', 'country').show(self.k)
        #result= result.select('outDegree','airport_id','city', 'country').groupBy('city').sum('outDegree')
        #result.orderBy(dataModel.col("sum(outDegree)").desc()).show(self.k)


# In[12]:


if __name__ == "__main__":
    k = int(input("How many airports do you want to display?"))
    ba= topBusyAirports(k)
    start_time= time.time()
    ba.inComing()
    ba.outGoing()
    end_time= time.time()
    print("time taken by parallel: {}".format(end_time-start_time))


# In[ ]:




