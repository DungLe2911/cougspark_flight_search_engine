import os,datetime
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext, SparkFiles
from graphframes import *



class ParGraphCityReac(object):

    def __init__(self, selection):
        self.path = os.path.join("../datasets", selection)
        #self.spark = SparkSession.builder.appName('cpts415-yize-bigdata').master('local').getOrCreate()
        self.spark = SparkSession.builder.appName('cpts415-yize-bigdata').master('spark://ip-172-31-59-32.us-west-2.compute.internal:7077').getOrCreate()
        self.AddFile()
        self.ReadFile()
        self.CreateGraph()

    def AddFile(self):
        self.spark.sparkContext.addFile(os.path.join(self.path, 'airports.csv'))
        self.spark.sparkContext.addFile(os.path.join(self.path, 'countries.csv'))
        self.spark.sparkContext.addFile(os.path.join(self.path, 'routes.csv'))

    def ReadFile(self):
        self.airports = self.spark.read.options(header=True, inferSchema=True, delimiter=',').csv(
            SparkFiles.get('airports.csv'))
        self.countries = self.spark.read.options(header=True, inferSchema=True, delimiter=',').csv(
            SparkFiles.get('countries.csv'))
        self.routes = self.spark.read.options(header=True, inferSchema=True, delimiter=',').csv(
            SparkFiles.get('routes.csv'))
        self.airports.createOrReplaceTempView("airports")
        self.countries.createOrReplaceTempView("countries")
        self.routes.createOrReplaceTempView('routes')

    def nodesReachability(self, start, end, z=1):
        # finding the reachability
        # problem translated from Finding the reachability between city
        return self.g.bfs('id=' + str(start), 'id=' + str(end), maxPathLength=z).collect()

    def GetAirportNameFromId(self,airport_id):
        return self.airports.filter("airport_id == {}".format(airport_id))

    def CreateGraph(self):
        self.airports = self.airports.withColumnRenamed('airport_id', 'id')

        self.routes = self.routes.withColumnRenamed('src_id', 'src')
        self.routes = self.routes.withColumnRenamed('dst_id', 'dst')

        v = self.airports
        e = self.routes
        self.g = GraphFrame(v, e)

    def cityReachability(self, city1, city2, z=1):
        # Find the reachability between city 
        # Need to convert city into nodes

        city1str = "\"" + city1 +"\""
        query_city1 = ''' select airport_id, city from airports Where City = %s ''' % city1str


        airportlist1 = []
        [airportlist1.append(x[0]) for x in self.spark.sql(query_city1).collect()]

        airportlist_str_1 = list(map(self.GetAirportNameFromId, airportlist1))

        if len(airportlist1) == 0:
            raise NameError("No Airport Information Found On The Input Departure Location")




        city2str = "\"" + city2 +"\""
        query_city2 = ''' select airport_id, city from airports Where City = %s ''' % city2str

        airportlist2 = []
        [airportlist2.append(x[0]) for x in self.spark.sql(query_city2).collect()]

        airportlist_str_2 = list(map(self.GetAirportNameFromId, airportlist2))

        if len(airportlist2) == 0:
            raise NameError("No Airport Information Found On The Input Arrival Location")


        for airport_1 in airportlist1:
            for airport_2 in airportlist2:
                result = self.nodesReachability(airport_1, airport_2,z)
                if len(result) > 0:
                    return result

        return False

    
    def printPath(self,result):
        print('='*100)
        for path in result:
            count = 0
            for element in path :
                if count % 2 == 0:
                    if (count == len(path)-1):
                        print (element['name'], end = '')
                    else:
                        print (element['name'], end = ' ==> ')
                else:
                    print (element['airline_name'], end = ' ==> ')
                count += 1
            print()
            print('='*100)
            
def main():
    pagra = ParGraphCityReac('enriched')
    
    start = datetime.datetime.now()
    result = pagra.cityReachability('Shanghai', 'New York')
    pagra.printPath(result)
    end = datetime.datetime.now()
    elapsed = end - start
    print("Time elapsed: ",elapsed)





if __name__ == '__main__':
    main()
