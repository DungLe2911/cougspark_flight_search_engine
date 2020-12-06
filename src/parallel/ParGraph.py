import os, datetime
from pyspark.sql import SparkSession
from pyspark import SparkFiles, SparkContext
from graphframes import *

"""
Washinton State University 
Cpts415: Big Data
Dr.Jia YU

Space Flight Search Engine
Jongyun Kim 11701083
"""

class ParGraph(object):
    """
    A class used to implement flight search engine with parallel algorithm.
    """
    def __init__(self, directory):
        """
        A constructor that reads csv files and initialize graph
        """
        self.path = os.path.join("../../datasets", directory)
        self.spark = SparkSession.builder.appName("cpts415-bigdata").master('spark://28f8bc9eb629:7077').getOrCreate()
        self._AddFile()
        self._ReadFile()
        self._CreateGraph()
        
    def _AddFile(self):
        """
        A method to add files into spark context
        """
        self.spark.sparkContext.addFile(os.path.join(self.path, 'routes.csv'))
        self.spark.sparkContext.addFile(os.path.join(self.path, 'airports.csv'))
        self.spark.sparkContext.addFile(os.path.join(self.path, 'airlines.csv'))
        self.spark.sparkContext.addFile(os.path.join(self.path, 'countries.csv'))
        self.spark.sparkContext.addFile(os.path.join(self.path, 'planes.csv'))
        
    def _ReadFile(self):
        """
        A method to read files for spark sql
        """
        self.airports = self.spark.read.options(header=True, inferSchema=True, delimiter=',').csv(SparkFiles.get("airports.csv"))
        self.routes = self.spark.read.options(header=True, inferSchema=True, delimiter=',').csv(SparkFiles.get("routes.csv"))
        self.airports.createOrReplaceTempView("airports")
        self.routes.createOrReplaceTempView("routes")

    def _CreateGraph(self):
        """
        A method to create graph from spark sql
        """
        g_airports = self.airports.withColumnRenamed('airport_id', 'id')
        g_routes = self.routes.withColumnRenamed('src_id', 'src')
        g_routes = g_routes.withColumnRenamed('dst_id', 'dst')
        
        v = g_airports
        e = g_routes
        self.g = GraphFrame(v, e)
        
    def FindAirlineWithCodeShare(self):
        """
        A method to find a list of airlines operating with code share
        """
        query = """
                SELECT DISTINCT airline_name
                FROM routes
                WHERE codeshare == "Y"
                """
        
        codeshare = self.spark.sql(query)
        return [(row['airline_name']) for row in codeshare.collect()]
    
    
    def FindTripXToYLessThanZ(self, X, Y, Z):
        """
        A method to find a trip that connects X and Y with less than Z stops (constrained reachability).
        """
        assert (Z < 4 or Z > 0), "Z should be less than equal 3"
        
        if(Z==1):
            paths = "(x)-[e]->(y)"
            trips = self.g.find(paths).filter('x.id == '+str(X)).filter('y.id =='+str(Y))
            result = trips.select("x.id","y.id").dropDuplicates()
        elif(Z==2):
            paths = "(x)-[e]->(b); (b)-[e2]->(y)"
            trips = self.g.find(paths).filter('x.id == '+str(X)).filter('y.id =='+str(Y))
            result = trips.select("x.id","b.id","y.id").dropDuplicates()
        else:
            paths = "(x)-[e]->(b); (b)-[e2]->(c) ; (c)-[e3]->(y)"
            trips = self.g.find(paths).filter('x.id == '+str(X)).filter('y.id =='+str(Y))
            result = trips.select("x.id","b.id","c.id","y.id").dropDuplicates()
            
        return [row for row in result.collect()]
    
    def GetAirportNameFromId(self, airport_id):
        """
        A method to get airport name from airport id
        """
        return self.airports.filter("airport_id == {}".format(airport_id))
    

def main():
    pg = ParGraph('enriched')

    start = datetime.datetime.now()
    airline_list = pg.FindAirlineWithCodeShare()
    end = datetime.datetime.now()
    delta = end-start
    elipsed = int(delta.total_seconds() * 1000)
    print("elipsed:",elipsed)
    print(airline_list)

    start = datetime.datetime.now()
    trips = pg.FindTripXToYLessThanZ(3577, 2380, 3)
    end = datetime.datetime.now()
    delta = end-start
    elipsed = int(delta.total_seconds() * 1000)
    print("elipsed:",elipsed)
    for trip in trips:
        print(trip[0], pg.GetAirportNameFromId(trip[0]), end=" ")
        for i in range(1, len(trip)):
            print("->",trip[i], pg.GetAirportNameFromId(trip[i]), end=" ")
        print()


if __name__ == '__main__':
    main()
