import pandas as pd
import networkx as nx
import os
import datetime


class SeqGraphAgg(object):
    """
    A class used to implement flight search engine with sequential algorithm.
    """

    def __init__(self, directory):
        """
        A constructor that reads csv files and initialize graph
        """
        self._path = os.path.join("./datasets", directory)
        self.airlines = pd.read_csv(os.path.join(self._path, 'airlines.csv'))
        self.airports = pd.read_csv(os.path.join(self._path, 'airports.csv'))
        self.planes = pd.read_csv(os.path.join(self._path, 'planes.csv'))
        self.countries = pd.read_csv(os.path.join(self._path, 'countries.csv'))
        self.routes = pd.read_csv(os.path.join(self._path, 'routes.csv'))
        self._CreateGraph()

    def _CreateGraph(self):
        """
        A method to create graph from pandas dataframe
        """
        self.nodes = []
        self.edges = []
        for i, r in self.airports.set_index('airport_id').iterrows():
            self.nodes.append((i, r.to_dict()))
        for i, r in self.routes.set_index(['src_id', 'dst_id']).iterrows():
            self.edges.append((i[0], i[1], r.to_dict()))
        print('node ex: {}'.format(self.nodes[0]))
        print('edge ex: {}'.format(self.edges[0]))

        self.graph = self._CreateAdjacencyListGraph()

    def _CreateAdjacencyListGraph(self):
        """
        A method to create adjacency list graph
        """
        graph = dict()
        for nodes in self.nodes:
            graph[nodes[0]] = set()
        for edges in self.edges:
            graph[edges[0]].add(edges[1])
        return graph

    def FindAirportInCountry(self, X):
        airport_in_country_list = []
        for node in self.nodes:
            if(node[1]['country'] == X):
                airport_in_country_list.append(node[1]['name'])

        return airport_in_country_list

    def FindAirlineHavingXStop(self, X):
        airline_having_xstop_list = []
        for edge in self.edges:
            if(edge[2]['stops'] == int(X)):
                airline_having_xstop_list.append(edge[2]['airline_name'])

        airline_having_xstop_list = list(set(airline_having_xstop_list))
        airline_having_xstop_list.sort()
        return airline_having_xstop_list

    def FindAirlineWithCodeShare(self):
        """
        A method to find a list of airlines operating with code share
        """
        airline_with_codeshare_list = []
        for edge in self.edges:
            if(edge[2]['codeshare'] == 'Y'):
                airline_with_codeshare_list.append(edge[2]['airline_name'])

        airline_with_codeshare_list = list(set(airline_with_codeshare_list))
        airline_with_codeshare_list.sort()
        return airline_with_codeshare_list

    def FindCountryHasHighestAirport(self):
        country_with_airports_dict = {}
        for node in self.nodes:
            country = node[1]['country']
            if country in country_with_airports_dict.keys():
                country_with_airports_dict[country] = country_with_airports_dict.get(country) + 1
            else:
                country_with_airports_dict[country] = 1
        
        country_with_airports_list = list(country_with_airports_dict.items())
        country_with_airports_list.sort(key=lambda x: x[1], reverse=True)
        return country_with_airports_list[:10]

    def FindTopKBusyCity(self, K):
        airport_with_incoming_routes_dict = {}
        airport_with_outgoing_routes_dict = {}
        for edge in self.edges:
            incoming = edge[1]
            outgoing = edge[0]
            if(incoming in airport_with_incoming_routes_dict.keys()):
                airport_with_incoming_routes_dict[incoming] = airport_with_incoming_routes_dict.get(incoming) + 1
            else:
                airport_with_incoming_routes_dict[incoming] = 1
            if(outgoing in airport_with_outgoing_routes_dict.keys()):
                airport_with_outgoing_routes_dict[outgoing] = airport_with_outgoing_routes_dict.get(outgoing) + 1
            else:
                airport_with_outgoing_routes_dict[outgoing] = 1

        city_with_incoming_routes_dict = {}
        city_with_outgoing_routes_dict = {}


        for k, v in airport_with_incoming_routes_dict.items():
            incoming_city = self.airports.set_index('airport_id').at[int(k),'city']
            if(incoming_city in city_with_incoming_routes_dict.keys()):
                city_with_incoming_routes_dict[incoming_city] = city_with_incoming_routes_dict[incoming_city] + v
            else:
                city_with_incoming_routes_dict[incoming_city] = v

        for k2, v2 in airport_with_outgoing_routes_dict.items():
            outgoing_city = self.airports.set_index('airport_id').at[int(k2),'city']
            if(outgoing_city in city_with_outgoing_routes_dict.keys()):
                city_with_outgoing_routes_dict[outgoing_city] = city_with_outgoing_routes_dict[outgoing_city] + v2
            else:
                city_with_outgoing_routes_dict[outgoing_city] = v2

        city_with_incoming_routes_list = list(city_with_incoming_routes_dict.items())
        city_with_incoming_routes_list.sort(key=lambda x: x[1], reverse = True)
        city_with_outgoing_routes_list = list(city_with_outgoing_routes_dict.items())
        city_with_outgoing_routes_list.sort(key=lambda x: x[1], reverse = True)

        return (city_with_incoming_routes_list[:int(K)], city_with_outgoing_routes_list[:int(K)])

    def FindTripXCityToYCity(self, X, Y):
        airport_in_Xcity = []
        for node in self.nodes:
            if(node[1]['city'] == X):
                airport_in_Xcity.append(node[0])

        airport_in_Ycity = []
        for node in self.nodes:
            if(node[1]['city'] == Y):
                airport_in_Ycity.append(node[0])

        # print(airport_in_Xcity)
        # print(airport_in_Ycity)

        def BFS_SP(graph, start, goal): 
            explored = [] 
            queue = [[start]] 
            
            while queue: 
                path = queue.pop(0) 
                node = path[-1] 
                
                if node not in explored: 
                    neighbours = graph[node] 
                    
                    for neighbour in neighbours: 
                        new_path = list(path) 
                        new_path.append(neighbour) 
                        queue.append(new_path) 
                        
                        if neighbour == goal: 
                            return new_path
                    explored.append(node) 
        
            return

        graph_adj = self.graph

        found_path = []
        for airport_x in airport_in_Xcity:
            for airport_y in airport_in_Ycity:
                shortest_path = BFS_SP(graph_adj, airport_x, airport_y)
                if(shortest_path is not None):
                    found_path.append(shortest_path)

        if(len(found_path) > 0):
            found_path.sort(key=lambda x: len(x))
            return [(i,self.GetAirportNameFromAirportId(i)) for i in found_path[0]]
        else:
            return []

    def FindTripXToYLessThanZ(self, X, Y, Z):
        """
        A method to find a trip that connects X and Y with less than Z stops (constrained reachability).
        """
        # G = nx.Graph()
        # G.add_nodes_from(self.nodes)
        # G.add_edges_from(self.edges)

        # paths_list = list(nx.all_simple_paths(G, X, Y, Z))
        # paths_list.sort(key = lambda x: len(x))

        # return paths_list
        X = int(X)
        Y = int(Y)
        Z = int(Z)
        graph_adj = self.graph
        visited = dict()
        for node in self.nodes:
            visited[node[0]] = False
        current_path = []
        simple_path = []

        def DFS(u, v, d):
            if (visited[u]):
                return
            visited[u] = True
            current_path.append(u)
            if(u == v):
                simple_path.append(current_path.copy())
                visited[u] = False
                current_path.pop()
                return
            if(d != 0):
                for next_node in graph_adj[u]:
                    DFS(next_node, v, d-1)
            current_path.pop()
            visited[u] = False

        DFS(X, Y, Z)

        simple_path.sort(key=lambda x: len(x))
        
        if(len(simple_path) > 0):
            return [(i, self.GetAirportNameFromAirportId(int(i))) for i in simple_path[0]]
        else:
            return []

    def FindDHopCities(self, d, X):
        """
        A method to find all the cities reachable within d hops of a city (bounded reachability). 
        """
        # G = nx.Graph()
        # G.add_nodes_from(self.nodes)
        # G.add_edges_from(self.edges)

        # airports_id_in_city = self.airports.loc[self.airports['city'] == X, 'airport_id'].to_list()

        # cities_h_hop = set()
        # for airport in airports_id_in_city:
        #     airports_h_hop = nx.descendants_at_distance(G, airport, h)
        #     for airport_h_hop in airports_h_hop:
        #         cities_h_hop.add(self.GetCityFromAirportId(airport_h_hop))

        # return cities_h_hop
        d = int(d)

        graph_adj = self.graph

        airports_id_in_city = self.airports.loc[self.airports['city'] == X, 'airport_id'].to_list(
        )
        cities_d_hop = set()
        for airport in airports_id_in_city:
            airports_d_hop = set()
            current_distance = 0
            queue = {airport}
            visited = {airport}

            # BFS
            while queue:
                if current_distance <= d and current_distance > 0:
                    airports_d_hop.update(queue)

                current_distance += 1

                current_path = set()
                for poped_node in queue:
                    for child in graph_adj[poped_node]:
                        if child not in visited:
                            visited.add(child)
                            current_path.add(child)

                queue = current_path

            for airport_d_hop in airports_d_hop:
                cities_d_hop.add(self.GetCityFromAirportId(airport_d_hop))

        cities_d_hop_list = list(cities_d_hop)
        cities_d_hop_list.sort()
        return cities_d_hop_list

    def GetAirportNameFromAirportId(self, airport_id):
        """
        A method to get airport name from airport id
        """
        return self.airports.set_index('airport_id').loc[airport_id]['name']

    def GetCityFromAirportId(self, airprot_id):
        """
        A method to get city from airport id
        """
        return self.airports.set_index('airport_id').loc[airprot_id]['city']

    def ToyApp(self, X, Y):
        airport_in_Xcity = []
        for node in self.nodes:
            if(node[1]['city'] == X):
                airport_in_Xcity.append(node[0])

        airport_in_Ycity = []
        for node in self.nodes:
            if(node[1]['city'] == Y):
                airport_in_Ycity.append(node[0])

        G = nx.Graph()
        G.add_nodes_from(self.nodes)
        G.add_edges_from(self.edges)

        found_path = []
        for airport_x in airport_in_Xcity:
            for airport_y in airport_in_Ycity:
                shortest_path = list(nx.all_simple_paths(G, airport_x, airport_y, 3))
                if(len(shortest_path) >0):
                    found_path.extend(shortest_path)

        found_path.sort(key=lambda x:len(x))

        if(len(found_path) > 0):
            found_path.sort(key=lambda x: len(x))
            plots = []
            for path in found_path[:5]:
                lat = []
                lon = []
                for i in path:
                    lat.append(self.GetLatitudeFromAirportId(i))
                    lon.append(self.GetLongitudeFromAirportId(i))
                plots.append((lat,lon))
            return plots
        else:
            return []

    def GetLatitudeFromAirportId(self, airport_id):
        return self.airports.set_index('airport_id').at[int(airport_id), "latitude"]
    def GetLongitudeFromAirportId(self, airport_id):  
        return self.airports.set_index('airport_id').at[int(airport_id), "longitude"]

def main():
    print("Load files and initalize graphs")
    start = datetime.datetime.now()
    sg = SeqGraphAgg('cleanedv2')
    end = datetime.datetime.now()
    delta = end-start
    elipsed = int(delta.total_seconds() * 1000)
    print("elipsed(ms):", elipsed)

    # airports_in_country = sg.FindAirportInCountry("South Korea")
    # print(airports_in_country)

    # airlines_having_xstop = sg.FindAirlineHavingXStop(1)
    # print(airlines_having_xstop)

    # airlines_with_codeshare = sg.FindAirlineWithCodeShare()
    # print(airlines_with_codeshare)

    # country_with_airports = sg.FindCountryHasHighestAirport()
    # print(country_with_airports)

    # topk_busy_incoming_city = sg.FindTopKBusyCity(10)[0]
    # topk_busy_outgoing_city = sg.FindTopKBusyCity(10)[1]

    # print(topk_busy_incoming_city)
    # print(topk_busy_outgoing_city)

    # print(sg.FindTripXCityToYCity("Seattle", "Pohang"))

    # print("Find a trip that connects X and Y with less than Z stops")
    # # seatac to pohang airport less than 3 stops
    # start = datetime.datetime.now()
    # trips = sg.FindTripXToYLessThanZ(3577,2380,3)
    # end = datetime.datetime.now()
    # delta = end-start
    # elipsed = int(delta.total_seconds() * 1000)
    # print("elipsed:",elipsed)
    # for trip in trips:
    #     print(trip, ":", sg.GetAirportNameFromAirportId(trip[0]), end="")
    #     for i in range(1,len(trip)):
    #         print(" ->", sg.GetAirportNameFromAirportId(trip[i]), end="")
    #     print()

    # start = datetime.datetime.now()
    # print(sg.FindDHopCities(1, 'Seattle'))
    # end = datetime.datetime.now()
    # delta = end-start
    # elipsed = int(delta.total_seconds() * 1000)
    # print("elipsed:",elipsed)
    # print(cities_from_d_hop)

    print(sg.ToyApp("Seattle", "Pohang"))


if __name__ == '__main__':
    main()
