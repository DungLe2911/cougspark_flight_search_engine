import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark import SparkFiles, SparkContext
from graphframes import *


class ParGraphAgg(object):
    def __init__(self, directory):
        self.path = os.path.join("./datasets", directory)
        self.spark = SparkSession.builder.appName(
            "space-flight-search").master('spark://d7a5fdf2eacc:7077').getOrCreate()
        self.spark.sparkContext.setCheckpointDir('./checkpoints')
        self._AddFile()
        self._ReadFile()
        self._CreateGraph()

    def _AddFile(self):
        self.spark.sparkContext.addFile(os.path.join(self.path, 'routes.csv'))
        self.spark.sparkContext.addFile(
            os.path.join(self.path, 'airports.csv'))
        self.spark.sparkContext.addFile(
            os.path.join(self.path, 'airlines.csv'))
        self.spark.sparkContext.addFile(
            os.path.join(self.path, 'countries.csv'))
        self.spark.sparkContext.addFile(os.path.join(self.path, 'planes.csv'))

    def _ReadFile(self):
        self.airports = self.spark.read.options(
            header=True, inferSchema=True, delimiter=',').csv(SparkFiles.get("airports.csv"))
        self.routes = self.spark.read.options(
            header=True, inferSchema=True, delimiter=',').csv(SparkFiles.get("routes.csv"))
        self.countries = self.spark.read.options(
            header=True, inferSchema=True, delimiter=',').csv(SparkFiles.get("countries.csv"))
        self.airports.createOrReplaceTempView("airports")
        self.routes.createOrReplaceTempView("routes")
        self.countries.createOrReplaceTempView("countries")

    def _CreateGraph(self):
        g_airports = self.airports.withColumnRenamed('airport_id', 'id')
        g_routes = self.routes.withColumnRenamed('src_id', 'src')
        g_routes = g_routes.withColumnRenamed('dst_id', 'dst')

        v = g_airports
        e = g_routes
        self.g = GraphFrame(v, e)

    def FindAirportInCountry(self, X):
        query = """
                SELECT name
                From airports
                WHERE country = "{}"
                """.format(X)

        airports_in_city_df = self.spark.sql(query)
        airports_in_city_list = [row['name']
                                 for row in airports_in_city_df.collect()]
        return airports_in_city_list

    def FindAirlineHavingXStop(self, X):
        query = """ 
                SELECT DISTINCT airline_name 
                FROM routes
                WHERE stops = '{}'
                ORDER BY airline_name ASC
                """.format(X)

        airline_having_xstop_df = self.spark.sql(query)
        airline_having_xstop_list = [(row['airline_name'])
                                     for row in airline_having_xstop_df.collect()]
        return airline_having_xstop_list

    def FindAirlineWithCodeShare(self):
        query = """
                SELECT DISTINCT airline_name
                FROM routes
                WHERE codeshare = "Y"
                ORDER BY airline_name ASC
                """

        airline_with_codeshare_df = self.spark.sql(query)
        airline_with_codeshare_list = [
            (row['airline_name']) for row in airline_with_codeshare_df.collect()]
        return airline_with_codeshare_list

    def FindCountryHasHighestAirport(self):
        query = """
                SELECT country, count('airport_id') as airport_count 
                FROM airports 
                LEFT JOIN countries ON airports.country = countries.name   
                GROUP BY country
                ORDER BY airport_count DESC
                """
        country_with_airports_df = self.spark.sql(query)
        country_with_airports_list = [(row['country'], row['airport_count']) for row in country_with_airports_df.select(
            'country', 'airport_count').collect()[:10]]
        
        return country_with_airports_list

    def FindTopKBusyCity(self, K):
        indegree_df = self.g.inDegrees
        outdegree_df = self.g.outDegrees

        airports_joined = self.airports.join(indegree_df, self.airports.airport_id == indegree_df.id, 'inner').drop(indegree_df.id)
        airports_joined = airports_joined.join(outdegree_df, self.airports.airport_id == outdegree_df.id, 'inner').drop(outdegree_df.id)
        airports_joined.createOrReplaceTempView("airports_joined")

        query_in = """
                   SELECT city, SUM(inDegree) as incoming
                   FROM airports_joined
                   GROUP BY city
                   ORDER BY incoming DESC
                   """
        
        topk_incoming_busy_city_df = self.spark.sql(query_in)
        # topk_incoming_busy_city_df.show()
        query_out = """
                   SELECT city, SUM(outDegree) as outgoing
                   FROM airports_joined
                   GROUP BY city
                   ORDER BY outgoing DESC
                   """
        
        topk_outgoing_busy_city_df = self.spark.sql(query_out)
        # topk_outgoing_busy_city_df.show()

        topk_incoming_busy_city_list = [(row['city'],row['incoming']) for row in topk_incoming_busy_city_df.collect()[:int(K)]]
        topk_outgoing_busy_city_list = [(row['city'],row['outgoing']) for row in topk_outgoing_busy_city_df.collect()[:int(K)]]

        return (topk_incoming_busy_city_list ,topk_outgoing_busy_city_list)

    def FindTripXCityToYCity(self, X, Y):
        query_x = """
                  SELECT airport_id, city 
                  FROM airports 
                  WHERE city = "{}"
                  """.format(X)
        query_y = """
                  SELECT airport_id, city 
                  FROM airports 
                  WHERE city = "{}"
                  """.format(Y)

        airport_in_Xcity = [row['airport_id'] for row in self.spark.sql(query_x).collect()]
        airport_in_Ycity = [row['airport_id'] for row in self.spark.sql(query_y).collect()]

        found_path = []
        for airport_x in airport_in_Xcity:
            distances = self.g.shortestPaths(landmarks = airport_in_Ycity).filter("id="+str(airport_x)).select('distances').collect()[0]['distances']
            if(len(distances) > 0):
                distances_list = list(distances.items())
                distances_list.sort(key=lambda x:x[1])
                found_path.append((airport_x, distances_list[0][0], distances_list[0][1]))
        
        if(len(found_path)>0):
            found_path.sort(key=lambda x:x[2])
            return self.FindTripXToYLessThanZ(found_path[0][0],found_path[0][1],found_path[0][2])
        else:
            return []

    def FindTripXToYLessThanZ(self, X, Y, Z):
        Z = int(Z)
        assert (Z < 4 and Z > 0), "Z should be less than equal 3"

        if(Z == 1):
            paths = "(x)-[e]->(y)"
            trips = self.g.find(paths).filter(
                'x.id == '+str(X)).filter('y.id =='+str(Y))
            result = trips.select("x.id", "y.id").dropDuplicates()
        if(Z == 2):
            paths = "(x)-[e]->(b); (b)-[e2]->(y)"
            trips = self.g.find(paths).filter(
                'x.id == '+str(X)).filter('y.id =='+str(Y))
            result = trips.select("x.id", "b.id", "y.id").dropDuplicates()
        if(Z == 3):
            paths = "(x)-[e]->(b); (b)-[e2]->(c) ; (c)-[e3]->(y)"
            trips = self.g.find(paths).filter(
                'x.id == '+str(X)).filter('y.id =='+str(Y))
            result = trips.select("x.id", "b.id", "c.id",
                                  "y.id").dropDuplicates()

        simple_path = result.collect()
        
        if(len(simple_path) > 0):
            return [(row, self.GetAirportNameFromAirportId(row)) for row in simple_path[0]]
        else:
            return []

    def FindDHopCities(self, d, X):
        d  = int(d)
        assert(d < 4 and d > 0), "d should be less than equal 3"
        query = """
                SELECT airport_id
                FROM airports
                WHERE city == "{}"
                """.format(X)

        airports_in_xcity = [row['airport_id'] for row in self.spark.sql(query).collect()]
        cities = []
        if (d > 0):
            paths = "(x)-[e]->(y)"
            for airport in airports_in_xcity:
                trips = self.g.find(paths).filter("x.id == "+str(airport)).filter("y.id !="+str(airport))
                cities.extend([row['city'] for row in trips.select("y.city", "y.country").dropDuplicates().collect()])
                
        if (d > 1):
            paths = "(x)-[e]->(b); (b)-[e2]->(y)"
            for airport in airports_in_xcity:
                trips = self.g.find(paths).filter("x.id == "+str(airport)).filter("y.id !="+str(airport))
                cities.extend([row['city'] for row in trips.select("y.city", "y.country").dropDuplicates().collect()])
                
        if (d > 2):
            paths = "(x)-[e]->(b); (b)-[e2]->(c) ; (c)-[e3]->(y)"
            for airport in airports_in_xcity:
                trips = self.g.find(paths).filter("x.id == "+str(airport)).filter("y.id !="+str(airport))
                cities.extend([row['city'] for row in trips.select("y.city", "y.country").dropDuplicates().collect()])

        cities = list(set(cities))
        cities.sort()
        return cities

    def ConnectedComponent(self):
        result = self.g.connectedComponents()
        result_df = result.select("id", "component").orderBy(col('component')).take(20)
        return [(row['id'],row['component']) for row in result_df]

    def GetAirportNameFromAirportId(self, airport_id):
        query = """
                SELECT DISTINCT name
                FROM airports
                WHERE airport_id == "{}"
                """.format(airport_id)

        airport_name = self.spark.sql(query)
        return airport_name.collect()[0]['name']

def main():
    pg = ParGraphAgg('cleanedv2')

    # airports_in_city = pg.FindAirportInCountry("Italy")
    # print(airports_in_city)

    # airlines_having_xstop = pg.FindAirlineHavingXStop("1")
    # print(airlines_having_xstop)

    # airlines_with_codeshare = pg.FindAirlineWithCodeShare()
    # print(airlines_with_codeshare)

    # country_with_airports = pg.FindCountryHasHighestAirport()
    # print(country_with_airports)

    # topk_busy_incoming_city = pg.FindTopKBusyCity(10)[0]
    # topk_busy_outgoing_city = pg.FindTopKBusyCity(10)[1]

    # print(topk_busy_incoming_city)
    # print(topk_busy_outgoing_city)

    # print(pg.FindTripXCityToYCity("Seattle", "Hamhung"))
    # trips = pg.FindTripXToYLessThanZ(3577, 2380, 3)

    # print(pg.FindDHopCities(1, 'Seattle'))

    print(pg.ConnectedComponent())

    

if __name__ == '__main__':
    main()
