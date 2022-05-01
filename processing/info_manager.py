import config
import pandas as pd
import os.path
import numpy as np
import json
from datetime import datetime

noise_data_DIR = ""


def write_state(state):
    print(state)
    with open("./temp/state.json", "w") as outfile: 
            json.dump(state, outfile)

def check_api():
    new_data = pd.read_csv(f"{noise_data_DIR}/Braila_noise_sensors.csv", index_col=0)
    if (os.path.exists("./temp/noise_sensors.csv")):
        old_data = pd.read_csv("./temp/noise_sensors.csv", index_col=0)
        old_nodes = old_data["INP NAME"]
        new_nodes = new_data["INP NAME"]
        change = list(set(new_nodes)-set(old_nodes)) # SMALLER - LARGER
        if change != []:
            to_append = new_data.loc[new_data['INP NAME'].isin(change)]
            print(to_append)
            old_data = old_data.append(to_append, ignore_index=True)
            print(old_data) 
            old_data.to_csv("./temp/noise_sensors.csv")
            return list(to_append["INP NAME"].to_numpy())
        else:
            return []
    else:
        new_data.to_csv("./temp/noise_sensors.csv")
        return []

def get_accessible_nodes():
    node_list = []
    my_file = open("./config/accessible_nodes.txt", "r")
    content_list = my_file.readlines()
    for entry in content_list:
        if entry[0].isnumeric():
            entry = "Jonctiune-" + entry
        node_list.append(entry[:-1])
    return node_list 

def write_logs(content):
    if os.path.exists("./storage/log.txt"):
        append_write = 'a' # append if already exists
    else:
        append_write = 'w' # make a new file if not

    f = open("./storage/log.txt", append_write)
    f.write("{0} -- {1}\n".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), content))
    f.close()
    return
