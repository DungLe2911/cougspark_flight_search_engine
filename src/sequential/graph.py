#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
import os

path= '../../work/Pinak/'
nodeFile= 'nodes.csv'
edgeFile= 'edges.csv'
nodePath= os.path.join(path,nodeFile)
edgePath= os.path.join(path,edgeFile)
nodes= pd.read_csv(nodePath, header=0)
edges= pd.read_csv(edgePath, header=0)
nodes= nodes.drop(columns=['Unnamed: 0'])
edges= edges.drop(columns=['Unnamed: 0'])

def addNode(v):
    global graph
    global nodeCount
    if v in graph:
        print("Vertex already exists: {}".format(v))
    else:
        nodeCount = nodeCount + 1
        graph[v] = []

def addEdge(v1, v2, e):
    global graph
    if v1 not in graph:
        print('Vertex not present: {}'.format(v1))
    elif v2 not in graph:
        print('Vertex not present: {}'.format(v2))
    else:
        edge= [v2, e]
        graph[v1].append(edge)

def printGraph():
    global graph
    for vertex in graph:
        for ed in graph[vertex]:
            print('{} --> {} : weight: {}'.format(vertex, ed[0], ed[1]))

graph={}
nodeCount=0
vertices= nodes['id'].to_numpy()
source= edges['src'].to_numpy()
destination= edges['dst'].to_numpy()

#add vertices
for j in range(len(vertices)):
    addNode(vertices[j])

#add edges and weights
for i in range(len(source)):
    v1= source[i]
    v2= destination[i]
    addEdge(v1,v2, i)

#printGraph()

