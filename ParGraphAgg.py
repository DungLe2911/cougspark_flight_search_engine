import os
from pyspark.sql import SparkSession
from pyspark import SparkFiles, SparkContext
from graphframes import *

class ParGraphAgg(object):
    def __init__(self, directory):
        self.path = os.path.join("./datasets", directory)
        self.spark = SparkSession.builder.appName("space-flight-search").master('spark://d7a5fdf2eacc:7077').getOrCreate()
        self._AddFile()
        self._ReadFile()
        self._CreateGraph()
        
    def _AddFile(self):
        self.spark.sparkContext.addFile(os.path.join(self.path, 'routes.csv'))
        self.spark.sparkContext.addFile(os.path.join(self.path, 'airports.csv'))
        self.spark.sparkContext.addFile(os.path.join(self.path, 'airlines.csv'))
        self.spark.sparkContext.addFile(os.path.join(self.path, 'countries.csv'))
        self.spark.sparkContext.addFile(os.path.join(self.path, 'planes.csv'))
        
    def _ReadFile(self):
        self.airports = self.spark.read.options(header=True, inferSchema=True, delimiter=',').csv(SparkFiles.get("airports.csv"))
        self.routes = self.spark.read.options(header=True, inferSchema=True, delimiter=',').csv(SparkFiles.get("routes.csv"))
        self.airports.createOrReplaceTempView("airports")
        self.routes.createOrReplaceTempView("routes")

    def _CreateGraph(self):
        g_airports = self.airports.withColumnRenamed('airport_id', 'id')
        g_routes = self.routes.withColumnRenamed('src_id', 'src')
        g_routes = g_routes.withColumnRenamed('dst_id', 'dst')
        
        v = g_airports
        e = g_routes
        self.g = GraphFrame(v, e)

    #########################################################
    # Add more functions
    def FindAirportInCountry(self, X):
        query = """
                SELECT name
                From airports
                WHERE country = "{}"
                """.format(X)
        
        airports_in_city_df = self.spark.sql(query)
        airports_in_city_list = [row['name'] for row in airports_in_city_df.collect()]
        return airports_in_city_list


    def FindDhopCities(self, X, d):
        query = """
                SELECT airport_id
                FROM airports
                WHERE city == "{}"
                """.format(X)

        cities = []
        airports = self.spark.sql(query)
        for airport in [row['airport_id'] for row in airports.collect()]:
            res = self.g.bfs("id = {}".format(airport), "id != \"{}\"".format(airport), maxPathLength=d)
            cities.extend([row['city'] for row in res.select("to.city").dropDuplicates().collect()])
        
        return cities
    
    #########################################################
        
    def FindAirlineWithCodeShare(self):
        query = """
                SELECT DISTINCT airline_name
                FROM routes
                WHERE codeshare == "Y"
                """
        
        codeshare = self.spark.sql(query)
        return [(row['airline_name']) for row in codeshare.collect()]
    
    
    def FindTripXToYLessThanZ(self, X, Y, Z):
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
        query = """
                SELECT DISTINCT name
                FROM airports
                WHERE airport_id == {}
                """.format(airport_id)
        
        airport_name = self.spark.sql(query)
        return airport_name.collect()[0]['name']

def main():
    pg = ParGraphAgg('cleanedv2')

    airports_in_city = pg.FindAirportInCountry("Italy")
    print(airports_in_city)
    # airline_list = pg.FindAirlineWithCodeShare()
    # print(airline_list)

    # trips = pg.FindTripXToYLessThanZ(3577, 2380, 3)
    # for trip in trips:
    #     print(trip[0], pg.GetAirportNameFromId(trip[0]), end=" ")
    #     for i in range(1, len(trip)):
    #         print("->",trip[i], pg.GetAirportNameFromId(trip[i]), end=" ")
    #     print()


if __name__ == '__main__':
    main()