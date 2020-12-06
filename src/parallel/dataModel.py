#!/usr/bin/env python
# coding: utf-8

# In[1]:

import networkx as nx
import matplotlib.pyplot as plt
import findspark
from pyspark import SparkContext, SparkConf
findspark.init()

# In[2]:
if SparkContext.getOrCreate().master == 'local[*]':
    SparkContext.getOrCreate().stop()
    conf = SparkConf().setAppName("cpts415-bigdata-DataModel").setMaster('spark://ip-172-31-59-32.us-west-2.compute.internal:7077')
    sc= SparkContext.getOrCreate(conf)
else:
    sc= SparkContext.getOrCreate()

# In[3]:
from graphframes import *
from pyspark.sql import SQLContext, SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql import Row
from pyspark.sql.types import IntegerType
from functools import reduce
from pyspark.sql.window import Window
# In[4]:
print("Checking cluster configuration")
print(sc)
# In[5]:

spark = SparkSession(sc)
sqlContext= SQLContext(sc)


# In[6]:

airports= sqlContext.read.option("header", True).csv("../../work/datasets/enriched/airports.csv")
routes= sqlContext.read.option("header", True).csv("../../work/datasets/enriched/routes.csv")

# # Vertex Creation

# In[7]:
gVertices= airports.select("airport_id", "name", "city", "country")
oldNames= gVertices.schema.names
newNames= ["id", "name", "city", "country"]
gVertices= reduce(lambda data, index: data.withColumnRenamed(oldNames[index], newNames[index]), range(len(oldNames)), gVertices)
#gVertices.show(10)


# In[8]:


gVertices= gVertices.withColumn("property", concat(gVertices.name, lit('|'), gVertices.city, lit('|'), gVertices.country))
gVertices= gVertices.drop('name').drop('city').drop('country')
#gVertices.show(10)


# # Edge Creation

# In[9]:
#routes.show(5)
# In[14]:


gEdges= routes.select('src_id', 'dst_id', 'airline_id', 'airline_name', 'stops', 'codeshare', 'equipments')
gEdges= gEdges.withColumn('airlineInfo', concat(gEdges.airline_id, lit('|'), gEdges.airline_name, lit('|'), gEdges.stops, lit('|'), gEdges.codeshare, lit('|'), gEdges.equipments ) )
gEdges= gEdges.drop('airline_id').drop('airline_name').drop('stops').drop('codeshare').drop('equipments')
oldNames= gEdges.schema.names
newNames= ['src', 'dst', 'airlineInformation']
gEdges= reduce(lambda data, index: data.withColumnRenamed(oldNames[index], newNames[index]), range(len(oldNames)), gEdges)
#gEdges.show()


# In[15]:


#gVertices.toPandas().to_csv('../../work/Pinak/nodes.csv')
#gEdges.toPandas().to_csv('../../work/Pinak/edges.csv')


# # Graph Creation and Statistics

# In[16]:
graph= GraphFrame(gVertices, gEdges)
# # Count of Edges and Vertices

# In[17]:

vCount= graph.vertices.count()
eCount= graph.edges.count()

print("graph load complete...")
print("Number of Airports (Vertices): {}".format(vCount))
print("Number of Flights (Edges): {}".format(eCount))

# # Degree of Graph
# # Graph Plot
# In[19]:


def plotGraph(edgeList):
    gPlot= nx.Graph()
    for row in edgeList.edges.select('src', 'dst', 'airlineInformation').take(30):
        gPlot.add_edge(row['src'], row['dst'])
    
    edge_labels= edgeList.edges.select('src', 'dst','airlineInformation').take(30)
    formattedEL= {(elem[0], elem[1]): elem[2] for elem in edge_labels}
    size= 1000
    font= 10
    plt.figure(figsize= (20,20))
    plt.title('Graph of 30 Aiports with Airline information as Edge Labels')
    pos= nx.spring_layout(gPlot)
    nx.draw_networkx_nodes(gPlot, pos, node_size= size, node_color= 'Purple')
    nx.draw_networkx_edges(gPlot, pos, width= 2.0, edge_color= 'black', style= 'solid')
    nx.draw_networkx_labels(gPlot, pos, font_size= font, font_color='white')
    nx.draw_networkx_edge_labels(gPlot, pos, edge_labels=formattedEL, font_size= 8, font_weight= 'bold', label_pos=0.5,  font_color='red')


# In[20]:
#plotGraph(graph)