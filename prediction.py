### algorithm module
### init, prediction, recalibration
from math import ceil
from tabnanny import check
import numpy as np
import pandas as pd
import config
import heapq
from itertools import combinations
import scipy
import random
from src import geo_converter as geo, approximator as calc
from scipy import optimize
import logging

LOGGER = logging.getLogger(__name__)
layout = pd.read_json(f"{config.layout_DIR}clean_network.json", orient="index")
diameter_info = pd.read_json("./layout/diameter_info.json", orient="index")
node_list = []

my_file = open("./layout/accessible_nodes.txt", "r")
content_list = my_file.readlines()
for entry in content_list:
    node_list.append(entry)

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

def calculate_distances(graph, starting_vertex):
    distances = {vertex: float('infinity') for vertex in graph}
    distances[starting_vertex] = 0
    pq = [(0, starting_vertex)]
    while len(pq) > 0:
        current_distance, current_vertex = heapq.heappop(pq)
        #LOGGER.info("CURRENT ", current_vertex)
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
        #LOGGER.info("CURRENT ", current_vertex)
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

def get_distance(source, endpoint):
    if type(source) == list:
        x1, y1 = source
    else: 
        x1, y1 = layout.loc[source].x, layout.loc[source].y
    x2, y2 = layout.loc[endpoint].x, layout.loc[endpoint].y
    distance = np.sqrt(((x2-x1))**2 + ((y2-y1))**2)
    return distance

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

def hill_crawlerv2(strength_map, state):
    node_list = []

    # Finding node which currently has the highest noise value
    high_idx = np.where(strength_map.strength == max(strength_map.strength))
    if len(strength_map.iloc[high_idx].index) > 1:
        current_peak = strength_map.iloc[high_idx[0][0]].index[0]
    else:
        current_peak = strength_map.iloc[high_idx].index[0]
    LOGGER.info(current_peak)

    # Checking if previous crawl resulted in new peak
    if state["crawl_res"]:
        previous_res = state["crawl_res"]
        LOGGER.info(previous_res)
        if current_peak not in previous_res:
            return current_peak


    # Loading in the accessible nodes
    my_file = open("./layout/accessible_nodes.txt", "r")
    content_list = my_file.readlines()
    for entry in content_list:
        node_list.append(entry[:-1])

    new_nodes = []
    
    # Finding a neighbor node for every direction
    for neighbor in layout.loc[current_peak]["connections"]:
        next = None
        checked = [neighbor]
        queue = []
        if neighbor not in node_list: # if node is not accessible for plantation - check its neighbors
                LOGGER.info(f"node {neighbor} is not accessible for plantation - check its neighbors")
                for edge in layout.loc[neighbor]["connections"]:
                    if edge not in checked: 
                        queue.append(edge)

        elif neighbor in node_list and neighbor not in strength_map.index.tolist(): # if node is accessible but already checked - end
            break
        else: 
            next = neighbor 

        while not next:
            # Iteratively check all connections until one is accessible
            for node in queue:
                if node in node_list and node not in strength_map.index.tolist() and node not in new_nodes:
                    LOGGER.info(f"FOUND NEXT NODE FOR {neighbor}: {node}")
                    next = node
                    break
                else:
                    checked.append(node)
                    for edge in layout.loc[node]["connections"]:
                        if edge not in checked:
                            queue.append(edge)
                        checked.append(edge)
                queue.remove(node)

                if len(queue) == 0:
                    LOGGER.info("No next node found!")
                    break
        
        new_nodes.append(next)
    
    if len(new_nodes) == 0: # if all surrounding nodes are checked, assume current peak is global
        new_nodes = current_peak
        
    return new_nodes
           
def get_angle_of_attack(line, node):
    x0 = line[1][0] - line[0][0]
    y0 = line[1][1] - line[0][1]
    direction_vector = (x0, y0)
    x1 = layout.loc[node].x - line[0][0]
    y1 = layout.loc[node].y - line[0][1]
    secondary_vector = (x1, y1)
    if secondary_vector == (0,0): return 0
    
    unit_vector_1 = direction_vector / np.linalg.norm(direction_vector)
    unit_vector_2 = secondary_vector / np.linalg.norm(secondary_vector)
    dot_product = round(np.dot(unit_vector_1, unit_vector_2), 4)
    angle = np.arccos(dot_product)
    angle = angle*180/np.pi

    return angle
    

def get_spread_info(source, endpoint):
    trail = get_trail(dijkstra_graph, source)
    distances = calculate_distances(dijkstra_graph, source)
    if type(endpoint) == str:
        vertex = endpoint
    elif type(endpoint) == list or type(endpoint) == tuple:
        distance = np.sqrt(((endpoint[0]-layout.loc[source].x))**2 + ((endpoint[1]-layout.loc[source].y))**2)
        return distance, 0
    distance_fall = distances[vertex]
    total_splits = np.product([len(layout.loc[x].connections)-1 for x in trail[vertex][:-1, 0] if len(layout.loc[x].connections) > 1])
    total_split = total_splits if total_splits > 0 else 1
    total_split_loss = -10*np.log10(1/total_splits)
    distance = sum([round(float(x), 5) for x in trail[vertex][1:, 1]])
    LOGGER.info(f"Distance of {distance} + division correction of {total_split_loss}")
    return distance, total_split_loss

def set_branches(strength_map, threshold_distance=200):
    origin = strength_map.loc[strength_map['strength'] == strength_map['strength'].max()][:1].index.item()
    next_highest_node = strength_map.sort_values(by=["strength"], ascending=False).iloc[1].name
    branches = {}
    nearby = layout.loc[origin].connections
    points = [[0, strength_map.loc[origin].strength]]
    appended = []
    accessible_nodes = node_list
    for c in combinations(nearby, 2):
        line_0 = [[layout.loc[origin].x, layout.loc[origin].y], [layout.loc[c[0]].x, layout.loc[c[0]].y]]
        line_1 = [[layout.loc[origin].x, layout.loc[origin].y], [layout.loc[c[1]].x, layout.loc[c[1]].y]]
        branch_0 = []
        branch_1 = []
        for node in layout.index:
            if node not in accessible_nodes: continue ####### (skip node if it's not accessible IRL or is virtual)
            if calculate_distances(dijkstra_graph, origin)[node] > threshold_distance: continue
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

# Sets leakage location in the middle between two nodes with highest signal - last case scenario
def failsafe(state):
    branches = {}
    branches["0"] = state["node_list"][:ceil(len(state["node_list"])/2)]
    branches["1"] = state["node_list"][ceil(len(state["node_list"])/2):]
    state["branches"] = branches
    return state

# Normalize noise level to pipe diameter
def level_normalization(origin, endpoint, signal, info_df=diameter_info, norm=50): ## norm of 50 is already in radius
    trail = get_trail(dijkstra_graph, origin)[endpoint]
    if endpoint==origin:
        try:
            radius = info_df.loc[f"{origin},{layout.loc[origin].connections[0]}"].diameter.item()/2
        except KeyError:
            radius = 50 ## most probable outcome
        input_signal_mwm = 10**(signal/10) ## convert from dB to mW/m^2
        signal_at_100 = input_signal_mwm*(norm**2/radius**2)
        normed_signal = 10*np.log10(signal_at_100/1)
        return normed_signal

    try:    
        if f"{trail[-1][0]},{trail[-2][0]}" in info_df.index:
            radius = info_df.loc[f"{trail[-1][0]},{trail[-2][0]}"].diameter.item()/2
            LOGGER.info(radius)
        elif  f"{trail[-1][0]},{trail[-3][0]}" in info_df.index:
            radius = info_df.loc[f"{trail[-1][0]},{trail[-3][0]}"].diameter.item()/2
            LOGGER.info(radius)
        else:
            radius = 50
            LOGGER.info("No entry for diameter found.")
    except IndexError:
        radius = 50
        LOGGER.info("Index error in diameter normalization step.")
    
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
    LOGGER.info(origin)
    for c in combinations(state["branches"].keys(), 2):
        branch_0 = state["branches"][c[0]]
        branch_1 = state["branches"][c[1]]
        ys = []
        xs = []
        for node in branch_0:
            xs.append(-get_distance(origin, node))
            LOGGER.info(origin, node)
            normed_strength = level_normalization(origin, node, strength_map.loc[node].strength)
            corrected_strength = normed_strength
            ys.append(corrected_strength)
        for node in branch_1:
            xs.append(get_distance(origin, node))
            normed_strength = level_normalization(origin, node, strength_map.loc[node].strength)
            corrected_strength = normed_strength
            ys.append(corrected_strength)
            
        coeff = np.polyfit(xs, ys, 3)
        poly = np.poly1d(coeff)
        new_x = np.linspace(min(xs), max(xs))
        new_y = poly(new_x)
        
        # Polynomial approximation to noise/distance curve
        def f(x):
            power = len(coeff)
            res = sum([coeff[n]*x**(power-n-1) for n in range(power)])
            return res

        f_max_x0 = scipy.optimize.fmin(lambda x: -f(x), 0)
        f_max = f(f_max_x0)[0]
        leakage_branch = branch_0[0] if f_max_x0 < 0 else branch_1[0] ### set to [1] when you add origin to branch
        LOGGER.info(f"Maximum reached at {f_max_x0}. Signal maximum: {f_max}")
        results.append([f_max_x0, f_max, leakage_branch])
        
    prediction = 0
    i = 0
    height = 0
    score = {node:0 for node in strength_map.index}
    location = {node:0 for node in strength_map.index}
    #LOGGER.info(score)
    for idx, res in enumerate(results):
        if res[1] > 70:
            LOGGER.info("Unable to accurately approximate.")
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
    
    LOGGER.info(f"leak is at {prediction/i} at a strength of {height/i}")
    
    return [loc_x, loc_y], leak_branch

# Depth of leakage along the branch
def get_depth_xy(origin, endpoint, depth):
    x0, y0 = layout.loc[origin].x, layout.loc[origin].y
    x1, y1 = layout.loc[endpoint].x, layout.loc[endpoint].y
    fi = np.arctan((y1-y0)/(x1-x0))
    loc_x, loc_y = round(x0+depth*np.cos(fi), 5), round(y0+depth*np.sin(fi), 5)
    return geo.raw_epsg3844_to_wgs84(loc_x, loc_y)

def multi_leak_case(strength_map, state, leak_branch):
    origin = strength_map.loc[strength_map['strength'] == strength_map['strength'].max()][:1].index.item()
    LOGGER.info(f"{leak_branch, state['branches'].values()}")
    for i in state["branches"].keys():
        if leak_branch in state["branches"][i]: leak_branch=i ### this changes leak_branch from node to index
    choices = list(state["branches"])
    choices.remove(leak_branch)
    second_branch = random.choice(choices)
    LOGGER.info(f"{choices, second_branch}")
    if len(state["branches"][second_branch]) < 2: 
        choices.remove(second_branch)
        second_branch = random.choice(choices)
        LOGGER.info(choices, second_branch)
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
    ## set intervals along the branch
    sections = np.linspace(0, 200, 4) ## 200 is distance to check
    res = calc.solve_double(points, sections)
    combined_leakages = {str(get_depth_xy(origin, x2, key)):[get_depth_xy(origin, x3, value)] for (key, value) in res.items()}

    return combined_leakages
