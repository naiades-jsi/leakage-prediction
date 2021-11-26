import layout_preproccess as lp
import pandas as pd
import json


layout = pd.read_json(f"./layout/clean_network.json", orient="index")
dijkstra_graph = lp.prepare_dijkstra_conn(layout)

with open("./layout/dijkstra.json", "w") as outfile: 
    json.dump(dijkstra_graph, outfile)