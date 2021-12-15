### algorithm module
### init, prediction, recalibration
import numpy as np
import pandas as pd
import config
import heapq
from itertools import combinations
import os
import scipy
import random
from processing import geo_converter as geo
from scipy import optimize


layout = pd.read_json(f"{config.layout_DIR}clean_network.json", orient="index")
diameter_info = pd.read_json("./layout/diameter_info.json", orient="index")

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

connections_array, names_array = make_connections(layout)

def prepare_dijkstra_conn(layout):
    connections_dict = {}
    for index, junction in layout.iterrows():
        
        name = junction.name
        connections_dict[name] = {}
        for conn in junction.connections:
            x2, y2 = layout.loc[conn].x, layout.loc[conn].y
            distance = np.sqrt(((junction.x-x2))**2 + ((junction.y-y2))**2)
            connections_dict[name][conn] = distance
            
    return connections_dict

dijkstra_graph = prepare_dijkstra_conn(layout)

def level_intersect(signals):
    origin = signals[0]
    
    for i in range(1,len(signals)):
        origin_int = 10**(origin/10)
        new = signals[i]
        if new == 0:
            continue
        new_int = 10**(new/10)
        addition = (origin_int + new_int)/origin_int
        origin = origin + 10*np.log10(addition)
        
    sound_level = origin    
    return sound_level

sim_fall_exp = 1.3

def make_connections(layout):
    connections_array = []
    names_array = []
    for idx, connection in enumerate(layout["connections"]):
        x1, y1 = layout.iloc[idx].x, layout.iloc[idx].y
        for junction in connection:
            if layout.iloc[idx].name == layout.loc[junction].name: continue
            if [layout.iloc[idx].name, layout.loc[junction].name] not in names_array and [layout.loc[junction].name, layout.iloc[idx].name] not in names_array:
                x2, y2 = layout.loc[junction].x, layout.loc[junction].y
                connections_array.append([x1, x2, y1, y2])
                names_array.append([layout.iloc[idx].name, layout.loc[junction].name])
    return connections_array, names_array
connections_array, names_array = make_connections(layout)

def prepare_dijkstra_conn(layout):
    connections_dict = {}
    for index, junction in layout.iterrows():
        
        name = junction.name
        connections_dict[name] = {}
        for conn in junction.connections:
            x2, y2 = layout.loc[conn].x, layout.loc[conn].y
            distance = np.sqrt(((junction.x-x2))**2 + ((junction.y-y2))**2)
            connections_dict[name][conn] = distance
            
    return connections_dict

def strength_function(x1, x2, y1, y2, current_strength, k):
    distance = np.sqrt(((x2-x1))**2 + ((y2-y1))**2)
    #distance = distance_deg * 1 ## approximate distance in m
    strength = current_strength - .001*distance**sim_fall_exp
    return strength

def set_point_of_leakage(connections, names, leakage_neigh="random"):
    
    if leakage_neigh == "random": 
        n_pipe = random.randint(0, len(connections)-1)
        leakage_pipe = connections[n_pipe]
    else: 
        leakage_pipe = layout.loc[leakage_neigh[0]].x, layout.loc[leakage_neigh[1]].x, layout.loc[leakage_neigh[0]].y, layout.loc[leakage_neigh[1]].y
        neigh_a, neigh_b = leakage_neigh
    leakage_depth = .5 #random.random()
    x1, x2, y1, y2 = leakage_pipe
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
    distances[starting_vertex] = 0
    pq = [(0, starting_vertex)]
    while len(pq) > 0:
        current_distance, current_vertex = heapq.heappop(pq)
        #print("CURRENT ", current_vertex)
        # Nodes can get added to the priority queue multiple times. We only
        # process a vertex the first time we remove it from the priority queue.
        if current_distance > distances[current_vertex]:
            continue
        for neighbor, weight in graph[current_vertex].items():
            distance = current_distance + weight
            # Only consider this new path if it's better than any path we've
            # already found.
            if distance < distances[neighbor]:
                distances[neighbor] = distance
                heapq.heappush(pq, (distance, neighbor))
                
    distances = {key:val for key, val in distances.items() if val != np.inf}
    return distances

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

def set_search_area(strength_map, perimeter_size, connections_map):
    search_area = {}
    search_pipes = []
    for sensor in strength_map.index:
        distance_map = calculate_distances(dijkstra_graph, sensor)
        nearest_20 = sorted(distance_map.items(), key=lambda item: item[1])[:perimeter_size] ## the sensor also
        search_area.update(set(nearest_20))
    search_area = list(search_area)
    already_seen = []
    for node in search_area:
        if "-A" in node:
            continue
        for neighbour in search_area:
            if neighbour in connections_map[node].keys() and neighbour not in already_seen:
                search_pipes.append([node, neighbour, connections_map[node][neighbour]])
                already_seen.append(neighbour)
        
            
    return search_pipes
        
    
def set_leakage_vertices(search_area):
    leakage_vertices = []
    for connection in search_area:
        x0, y0 = layout.loc[connection[0]].x, layout.loc[connection[0]].y
        x1, y1 = layout.loc[connection[1]].x, layout.loc[connection[1]].y
        new_leakage_vertex = ((x0+x1)/2, (y0+y1)/2)
        leakage_vertices.append(new_leakage_vertex)
    
    return np.array(leakage_vertices)    



def get_n_neighbours(node):
    return len(layout.loc[node].connections)

def get_distance(source, endpoint):
    if type(source) == list:
        x1, y1 = source
    else: 
        x1, y1 = layout.loc[source].x, layout.loc[source].y
    x2, y2 = layout.loc[endpoint].x, layout.loc[endpoint].y
    distance = np.sqrt(((x2-x1))**2 + ((y2-y1))**2)
    return distance

def decay_function(distance, threshold, exp=2): ## add exp for distance 
    decay_coeff = 1 - distance**exp/threshold
    return decay_coeff if decay_coeff > 0 else 0

def get_coeff(source, endpoint):
    if type(source) == list:
        n_neigh = 1
        dist = get_distance(source, endpoint)
        decay_coeff = decay_function(dist, 100000)
        k = 1/n_neigh*decay_coeff
    
    else: 
        n_neigh = get_n_neighbours(endpoint)
        dist = get_distance(source, endpoint)
        decay_coeff = decay_function(dist, 100000)
        k = 1/n_neigh*decay_coeff

    return k

def set_strength_map():
    init_param = {}
    strength_map = {}
    sheet = pd.read_csv("./temp/noise_sensors.csv")
    init_param["starting_sensors"] = []
    for entry in sheet.index:
        row = sheet.iloc[entry]["INP NAME"]
        init_param["starting_sensors"].append(row)
        strength_map[row] = [layout.loc[row].x, layout.loc[row].y, sheet.iloc[entry]["READING"]]
    return pd.DataFrame.from_dict(strength_map, orient="index", columns=["x", "y", "strength"])

class Missing(Exception): pass

def check_edges(edges, strength_map, peak):
    length = len(edges)
    found = True
    for node in edges:
        try:
            if node in strength_map.index:
                pass
            else:
                for conn in layout.loc[node].connections:
                    if conn in strength_map.index and node != peak:
                        pass
                    elif conn != peak and "-A" not in conn:
                        raise Missing
        
        except Missing:
            return False
    
    return found ### when FALSE - all edges were not found


def hill_crawler(strength_map):
    high_idx = np.where(strength_map.strength == max(strength_map.strength))
    #print(high_idx, high_idx[0][0])
    if len(strength_map.iloc[high_idx].index) > 1:
        current_peak = strength_map.iloc[high_idx[0][0]].name
    else:
        current_peak = strength_map.iloc[high_idx].name
    found = False
    relocations = 0
    
    print("Current peak: ", current_peak)
    edges = layout.loc[current_peak].connections
    print("Neighbours of ", current_peak, ": ", edges)

    if check_edges(edges, strength_map, current_peak):
        return current_peak

    
    to_check = []
    for edge in edges:
        if edge not in strength_map.index and "-A" not in edge: ### add a skip step (maybe next node is real and can be checked)
            relocations += 1
            to_check.append(edge)
        elif edge not in strength_map.index and "-A" in edge: ### add a skip step (maybe next node is real and can be checked)
            relocations += 1
            line = [[layout.loc[current_peak].x, layout.loc[current_peak].y], [layout.loc[edge].x, layout.loc[edge].y]]
            check_next = None
            while not check_next or "-A" in check_next:
                best = -10
                if check_next:
                   edge = check_next 
                for candidate in layout.loc[edge].connections:
                    line = [[layout.loc[current_peak].x, layout.loc[current_peak].y], [layout.loc[edge].x, layout.loc[edge].y]]
                    angle = get_angle_of_attack(line, candidate)
                    print(angle, check_next)
                    if best < angle:
                        best = angle
                        check_next = candidate

            if check_next not in strength_map:
                to_check.append(check_next)
    if len(to_check) == 0:
        return current_peak
    else:
        return to_check
    
            
def get_angle_of_attack(line, node):
    #print(line, node)
    x0 = line[1][0] - line[0][0]
    y0 = line[1][1] - line[0][1]
    direction_vector = (x0, y0)
    #print(direction_vector)
    x1 = layout.loc[node].x - line[0][0]
    y1 = layout.loc[node].y - line[0][1]
    secondary_vector = (x1, y1)
    #print(secondary_vector)
    if secondary_vector == (0,0): return 0
    
    unit_vector_1 = direction_vector / np.linalg.norm(direction_vector)
    unit_vector_2 = secondary_vector / np.linalg.norm(secondary_vector)
    dot_product = round(np.dot(unit_vector_1, unit_vector_2), 4)
    #print(dot_product)
    angle = np.arccos(dot_product)
    angle = angle*180/np.pi

    return angle
    

def get_spread_info(source, endpoint):
    trail = get_trail(dijkstra_graph, source)
    distances = calculate_distances(dijkstra_graph, source)
    if type(endpoint) == str:
        print("yes")
        vertex = endpoint
    elif type(endpoint) == list or type(endpoint) == tuple:
        distance = np.sqrt(((endpoint[0]-layout.loc[source].x))**2 + ((endpoint[1]-layout.loc[source].y))**2)
        return distance, 0
    distance_fall = distances[vertex]
    total_splits = np.product([len(layout.loc[x].connections)-1 for x in trail[vertex][:-1, 0] if len(layout.loc[x].connections) > 1])
    total_split = total_splits if total_splits > 0 else 1
    total_split_loss = -10*np.log10(1/total_splits)
    distance = sum([round(float(x), 5) for x in trail[vertex][1:, 1]])
    print(f"Distance of {distance} + division correction of {total_split_loss}")
    
    return distance, total_split_loss

def set_branches(strength_map):
    origin = strength_map.loc[strength_map['strength'] == strength_map['strength'].max()][:1].index.item()
    next_highest_node = strength_map.sort_values(by=["strength"], ascending=False).iloc[1].name
    branches = {}
    nearby = layout.loc[origin].connections
    points = [[0, strength_map.loc[origin].strength]]
    appended = []
    for c in combinations(nearby, 2):
        line_0 = [[layout.loc[origin].x, layout.loc[origin].y], [layout.loc[c[0]].x, layout.loc[c[0]].y]]
        line_1 = [[layout.loc[origin].x, layout.loc[origin].y], [layout.loc[c[1]].x, layout.loc[c[1]].y]]
        branch_0 = []
        branch_1 = []
        print(c[0], c[1])
        for node in layout.index:
            if "-A" in node: continue
            if calculate_distances(dijkstra_graph, origin)[node] > 200: continue
            angle_0 = get_angle_of_attack(line_0, node)
            angle_1 = get_angle_of_attack(line_1, node)
            if angle_0 < 4 and len(branch_0) < 4: branch_0.append(node)
            elif angle_1 < 4 and len(branch_1) < 4: branch_1.append(node)
        if c[0] not in appended:
            branches[len(branches)] = branch_0
            appended.append(c[0])
        if c[1] not in appended:
            branches[len(branches)] = branch_1
            appended.append(c[1])
    return branches

def level_normalization(origin, endpoint, signal, info_df=diameter_info, norm=50): ## norm of 50 is already in radius
    trail = get_trail(dijkstra_graph, origin)[endpoint]
    if endpoint==origin:
        radius = info_df.loc[f"{origin},{layout.loc[origin].connections[0]}"].diameter.item()/2
        input_signal_mwm = 10**(signal/10) ## convert from dB to mW/m^2
        signal_at_100 = input_signal_mwm*(norm**2/radius**2)
        normed_signal = 10*np.log10(signal_at_100/1)
        return normed_signal
        
    if f"{trail[-1][0]},{trail[-2][0]}" in info_df.index:
        radius = info_df.loc[f"{trail[-1][0]},{trail[-2][0]}"].diameter.item()/2
        print(radius)
    elif  f"{trail[-1][0]},{trail[-3][0]}" in info_df.index:
        radius = info_df.loc[f"{trail[-1][0]},{trail[-3][0]}"].diameter.item()/2
        print(radius)
    else:
        radius = 50
        print("No entry found.")
    
    input_signal_mwm = 10**(signal/10) ## convert from dB to mW/m^2
    signal_at_100 = input_signal_mwm*(norm**2/radius**2)
    normed_signal = 10*np.log10(signal_at_100/1)
    
    return normed_signal

leak_branch = None

def set_polynomes(strength_map, state):
    origin = strength_map.loc[strength_map['strength'] == strength_map['strength'].max()][:1].index.item()
    next_highest_node = strength_map.sort_values(by=["strength"], ascending=False).iloc[1].name
    results = []
    nearby = layout.loc[origin].connections
    points = [[0, strength_map.loc[origin].strength]]
    print(origin)
    for c in combinations(state["branches"].keys(), 2):
        branch_0 = state["branches"][c[0]]
        branch_1 = state["branches"][c[1]]
        
        
        
        ys = []
        xs = []
        for node in branch_0:
            xs.append(-get_distance(origin, node))
            print(origin, node)
            normed_strength = level_normalization(origin, node, strength_map.loc[node].strength)
            corrected_strength = normed_strength#strength_map.loc[node].strength #+ signal_correction(origin, node, next_highest_node=next_highest_node)
            ys.append(corrected_strength)
        for node in branch_1:
            xs.append(get_distance(origin, node))
            normed_strength = level_normalization(origin, node, strength_map.loc[node].strength)
            corrected_strength = normed_strength#strength_map.loc[node].strength #+ signal_correction(origin, node, next_highest_node=next_highest_node)
            ys.append(corrected_strength)
            
            
        
        coeff = np.polyfit(xs, ys, 3)
        poly = np.poly1d(coeff)
        new_x = np.linspace(min(xs), max(xs))
        new_y = poly(new_x)
        
        
        def f(x):
            power = len(coeff)
            res = sum([coeff[n]*x**(power-n-1) for n in range(power)])
            return res

        f_max_x0 = scipy.optimize.fmin(lambda x: -f(x), 0)
        f_max = f(f_max_x0)[0]
        leakage_branch = branch_0[0] if f_max_x0 < 0 else branch_1[0] ### set to [1] when you add origin to branch
        print("Maximum reached at ", f_max_x0, " Signal maximum: ", f_max)
        results.append([f_max_x0, f_max, leakage_branch])
        
    
    prediction = 0
    i = 0
    height = 0
    score = {node:0 for node in strength_map.index}
    location = {node:0 for node in strength_map.index}
    print(score)
    for idx, res in enumerate(results):
        if res[1] > 70:
            print("not able to approximate")
            
        else:
            
            height += res[1]
            i += 1
            score[res[2]] += 1
            location[res[2]] += res[0]
    
    leak_branch = max(score, key=score.get)
    leak_loc = (location[leak_branch]/score[leak_branch])[0]
    x0, y0 = layout.loc[origin].x, layout.loc[origin].y
    x1, y1 = layout.loc[leak_branch].x, layout.loc[leak_branch].y
    fi = np.arctan((y1-y0)/(x1-x0))
    
    loc_x, loc_y = round(x0+leak_loc*np.cos(fi), 5), round(y0+leak_loc*np.sin(fi), 5)
    
    
    print("leak is at", prediction/i, " at a strength of ", height/i)
    print(score, leak_branch, leak_loc)
    
        
    return [loc_x, loc_y], leak_branch


def get_depth_xy(origin, endpoint, depth):
    x0, y0 = layout.loc[origin].x, layout.loc[origin].y
    x1, y1 = layout.loc[endpoint].x, layout.loc[endpoint].y
    fi = np.arctan((y1-y0)/(x1-x0))
    loc_x, loc_y = round(x0+depth*np.cos(fi), 5), round(y0+depth*np.sin(fi), 5)
    
    return geo.raw_epsg3844_to_wgs84(loc_x, loc_y)

def multi_leak_case(strength_map, state, leak_branch):
    origin = strength_map.loc[strength_map['strength'] == strength_map['strength'].max()][:1].index.item()
    print(leak_branch, state["branches"].values())
    for i in state["branches"].keys():
        if leak_branch in state["branches"][i]: leak_branch=i ### this changes leak_branch from node to index
    choices = list(state["branches"])
    choices.remove(leak_branch)
    second_branch = random.choice(choices)
    print(choices, second_branch)
    if len(state["branches"][second_branch]) < 2: 
        choices.remove(second_branch)
        second_branch = random.choice(choices)
        print(choices, second_branch)
    x0 = state["branches"][second_branch][0]
    x1 = state["branches"][second_branch][1]
    x2 = state["branches"][leak_branch][0]
    x3 = state["branches"][leak_branch][1]
    ## spremeni x0 = ime noda ---> x = distance od origina, y = strength_map[x0]
    points = []
    points.append([-get_distance(origin, x0), strength_map.loc[x0].strength])
    points.append([-get_distance(origin, x1), strength_map.loc[x1].strength])
    points.append([get_distance(origin, x2), strength_map.loc[x2].strength])
    points.append([get_distance(origin, x3), strength_map.loc[x3].strength])
    ## nastavi odseke po leak branchu
    sections = np.linspace(0, 200, 4) ## 200 is distance to check
    from processing import calculator as calc
    res = calc.solve_double(points, sections)
    combined_leakages = {str(get_depth_xy(origin, x2, key)):[get_depth_xy(origin, x3, value)] for (key, value) in res.items()}

    return combined_leakages
