#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sys
import time
path="../../work/Pinak/"
sys.path.insert(0,path)
import graph
from operator import itemgetter


# In[2]:


class topBusyAirports:
    
    def __init__(self, k=3):
        self.k= k
        self.g= graph.graph
        self.outList=[0]*len(self.g)
        self.inList=[0]*len(self.g)
        self.airportList= self.g.keys()
        self.airportList= list(self.airportList)
    
    def calculateInOutDegree(self):
        for x,y in self.g.items():
            ind= self.airportList.index(x)
            self.outList[ind]= len(y)
            apItems= list( map(itemgetter(0), y ))
            for pos in range(0, len(apItems)):
                ind= self.airportList.index(x)
                self.inList[ind]+=1
    
    def getBusyIncomingAirports(self):
        self.calculateInOutDegree()
        incoming= sorted(zip(self.inList, self.airportList), reverse=True)[:k]
        inAp=[]
        for x in incoming:
            inAp.append(x[1])
        return inAp
        
    def getBusyOutgoingAirports(self):
        self.calculateInOutDegree()
        outgoing= sorted(zip(self.outList, self.airportList), reverse=True)[:k]
        outAp=[]
        for x in outgoing:
            outAp.append(x[1])
        return outAp
        


# In[3]:


if __name__ == "__main__":
    k = int(input("How many airports do you want to display?"))
    ba= topBusyAirports(k)
    start_time= time.time()
    busy_in_id= ba.getBusyIncomingAirports()
    busy_out_id= ba.getBusyOutgoingAirports()
    end_time= time.time()
    print("busy incoming airports: {}".format(busy_in_id))
    print("busy outgoing airports: {}".format(busy_out_id))
    print("time taken by Sequential Algorithm: {}".format(end_time-start_time))


# In[4]:


1200.6677343845367/60


# In[ ]:




