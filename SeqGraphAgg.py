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
        # print('node ex: {}'.format(self.nodes[0]))
        # print('edge ex: {}'.format(self.edges[0]))

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
        codeshare_airline_id_list = []
        for edge in self.edges:
            if(edge[2]['codeshare'] == 'Y'):
                codeshare_airline_id_list.append(edge[2]['airline_id'])

        codeshare_airline_id_list = set(codeshare_airline_id_list)

        codeshare_airline_name_list = []
        for airline_id in codeshare_airline_id_list:
            codeshare_airline_name_list.append(
                self.airlines.set_index('airline_id').loc[airline_id]['name'])

        return codeshare_airline_name_list

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

        return simple_path

    def FindDHopCities(self, X, d):
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
                if current_distance == d:
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

        return cities_d_hop

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


def main():
    print("Load files and initalize graphs")
    start = datetime.datetime.now()
    sg = SeqGraphAgg('cleanedv2')
    end = datetime.datetime.now()
    delta = end-start
    elipsed = int(delta.total_seconds() * 1000)
    print("elipsed(ms):", elipsed)

    airports_in_country = sg.FindAirportInCountry("South Korea")
    print(airports_in_country)

    airlines_having_xstop = sg.FindAirlineHavingXStop(1)
    print(airlines_having_xstop)

    # print("Find a list of airlines operating with code share")
    # start = datetime.datetime.now()
    # codeshare_airlines = sg.FindAirlineWithCodeShare()
    # end = datetime.datetime.now()
    # delta = end-start
    # elipsed = int(delta.total_seconds() * 1000)
    # print("elipsed:",elipsed)
    # print(codeshare_airlines)

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
    # cities_from_d_hop = sg.FindDHopCities('Seattle',2)
    # end = datetime.datetime.now()
    # delta = end-start
    # elipsed = int(delta.total_seconds() * 1000)
    # print("elipsed:",elipsed)
    # print(cities_from_d_hop)


if __name__ == '__main__':
    main()
