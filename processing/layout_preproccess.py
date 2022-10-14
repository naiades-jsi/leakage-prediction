import numpy as np
import random
import heapq


def make_connections(layout):
    connections_array = []
    names_array = []
    for idx, connection in enumerate(layout["connections"]):
        x1, y1 = layout.iloc[idx].x, layout.iloc[idx].y
        for junction in connection:
            if [layout.iloc[idx].name, layout.loc[junction].name] not in names_array and [layout.loc[junction].name, layout.iloc[idx].name] not in names_array:
                x2, y2 = layout.loc[junction].x, layout.loc[junction].y
                connections_array.append([x1, x2, y1, y2])
                names_array.append([layout.iloc[idx].name, layout.loc[junction].name])
    return connections_array, names_array

def prepare_dijkstra_conn(layout):
    connections_dict = {}
    for index, junction in layout.iterrows():
        
        name = junction.name
        connections_dict[name] = {}
        for conn in junction.connections:
            x2, y2 = layout.loc[conn].x, layout.loc[conn].y
            distance = np.sqrt(((junction.x-x2)*11.32)**2 + ((junction.y-y2)*11.32)**2)
            connections_dict[name][conn] = distance
            
    return connections_dict

def strength_function(x1, x2, y1, y2, current_strength, k):
    distance = np.sqrt(((x2-x1)*11.32)**2 + ((y2-y1)*11.32)**2)
    #distance = distance_deg * 11.32 ## approximate distance in m
    strength = current_strength - distance**0.07
    return strength

def set_point_of_leakage(connections, names):
    n_pipe = random.randint(0, len(connections)-1)
    leakage_pipe = connections[n_pipe]
    leakage_depth = random.random()
    x1, x2, y1, y2 = leakage_pipe
    neigh_a, neigh_b = names[n_pipe]
    x_of_leakage = x1*(1-leakage_depth) + x2*leakage_depth
    y_of_leakage = y1*(1-leakage_depth) + y2*leakage_depth
    return [x_of_leakage, y_of_leakage], [neigh_a, neigh_b]

def set_primary_signal(layout, point_of_leakage, neighbour_junctions, leakage_strength):
    strength_map = {}
    x1, y1 = point_of_leakage
    for junction in neighbour_junctions:
        x2, y2 = layout.loc[junction].x, layout.loc[junction].y
        strength_map[junction] = [x2, y2, strength_function(x1, x2, y1, y2, leakage_strength, 1)]
    return strength_map

def calculate_distances(graph, starting_vertex):
    distances = {vertex: float('infinity') for vertex in graph}
    joints = {vertex: 0 for vertex in graph}
    distances[starting_vertex] = 0
    joints[starting_vertex] = 0
    pq = [(0, starting_vertex, 0)]
    while len(pq) > 0:
        current_distance, current_vertex, current_joint = heapq.heappop(pq)
        #print("CURRENT ", current_vertex)
        # Nodes can get added to the priority queue multiple times. We only
        # process a vertex the first time we remove it from the priority queue.
        if current_distance > distances[current_vertex]:
            continue
        for neighbor, weight in graph[current_vertex].items():
            distance = current_distance + weight
            n_joints = current_joint + 1
            # Only consider this new path if it's better than any path we've
            # already found.
            if distance < distances[neighbor]:
                distances[neighbor] = distance
                joints[neighbor] = n_joints
                heapq.heappush(pq, (distance, neighbor, n_joints))
                
    distances = {key:val for key, val in distances.items() if val != np.inf}
    return distances, joints

def get_trail(graph, starting_vertex):
    distances = {vertex: float('infinity') for vertex in graph}
    trail = {vertex: [starting_vertex] for vertex in graph}
    distances[starting_vertex] = 0
    trail[starting_vertex] = [starting_vertex, 0]
    pq = [(0, starting_vertex, [[starting_vertex, 0]])]
    while len(pq) > 0:
        current_distance, current_vertex, temp = heapq.heappop(pq)
        #print("CURRENT ", current_vertex)
        # Nodes can get added to the priority queue multiple times. We only
        # process a vertex the first time we remove it from the priority queue.
        if current_distance > distances[current_vertex]:
            continue
        for neighbor, weight in graph[current_vertex].items():
            distance = current_distance + weight
            tempo = np.append(temp, [[neighbor, weight]], axis=0)
            # Only consider this new path if it's better than any path we've
            # already found.
            if distance < distances[neighbor]:
                distances[neighbor] = distance
                trail[neighbor] = tempo
                heapq.heappush(pq, (distance, neighbor, tempo))
                
    distances = {key:val for key, val in distances.items() if val != np.inf}
    return trail

faulty = []

def new_strength_map(dijkstra_graph, leakage_strength, layout, connections, names):
    point_of_leakage, neigh = set_point_of_leakage(connections, names)
    strength_map = set_primary_signal(layout, point_of_leakage, neigh, leakage_strength)
    
    for start_junction in neigh:
        start_strength = strength_map[start_junction][-1]
        trail = get_trail(dijkstra_graph, start_junction)
        for endpoint in trail:
            if endpoint == start_junction: continue
            try: ## this is to catch currently isolated junctions
                    #print("EDNOINOAN", endpoint)
                    #print(trail[endpoint])
                total_strength_fall = np.sum([float(x)**0.07 for x in trail[endpoint][:, -1]])
                ## total_junction_fall is fall in strength due to pipe division in junction 
                ## calculated with 10*log10(1/split) 
                total_junction_fall = sum([-10*np.log10(1/(len(layout.loc[x].connections)-1)) for x in trail[endpoint][:-1, 0] if len(layout.loc[x].connections)-1 != 0])
                #total_junction_fall = -10*np.log10(1/total_splits)
                total_strength = start_strength - total_strength_fall - total_junction_fall
                if total_strength < 0: total_strength = 0
                if endpoint in strength_map.keys() and total_strength > strength_map[endpoint][-1]:
                    x, y = layout.loc[endpoint].x, layout.loc[endpoint].y
                    strength_map[endpoint] = [x, y, total_strength]
                elif endpoint not in strength_map.keys():
                    x, y = layout.loc[endpoint].x, layout.loc[endpoint].y
                    strength_map[endpoint] = [x, y, total_strength]
            except TypeError as e:
                if endpoint not in faulty:
                    faulty.append(endpoint)
                    print(endpoint, trail[endpoint])
    return strength_map, point_of_leakage