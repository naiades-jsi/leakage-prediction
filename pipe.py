import prediction as prediction
import pandas as pd
from processing import geo_converter as geo





def run_crawler(state):
    if not state:
        state = {
            "iter": -1,
            "crawl_complete" : False,
            "crawl_res" : None,
            "converged" : False,
            "algorithm_res" : None,
            "branches": {},
            "to_append": []
        }
        strength_map = prediction.set_strength_map()
        strength_map.to_csv("./temp/strength_map.csv")

    else:
        strength_map = pd.read_csv("./temp/strength_map.csv", index_col=0)
        state = state
    res = prediction.hill_crawler(strength_map)

    if type(res) == str:
        print("Peak found at ", res)
        state["iter"] += 1
        state["crawl_complete"] = True
        state["crawl_res"] = res
        return state

    elif type(res) == list:
        print("To check ", res)
        state["iter"] += 1
        state["crawl_complete"] = False
        state["crawl_res"] = res

    return state



def run_calibrate(state):
    state = state
    strength_map = pd.read_csv("./temp/strength_map.csv", index_col=0)
    noise_sensors = pd.read_csv("./temp/noise_sensors.csv")
    common = state["predicted_res"]
    new_signal = noise_sensors["READING"].iloc[-1]
    new_fall_exp, strength_map = prediction.calibrate(strength_map, common, new_signal, randomize=False)
    state["calibration"] = True
    
    try:
        state["fall_exp"] = new_fall_exp.tolist()[0]
        state["calibration_res"] = new_fall_exp.tolist()[0]
    except TypeError:
        state["fall_exp"] = new_fall_exp.tolist()
        state["calibration_res"] = new_fall_exp.tolist()

    strength_map.to_csv("./temp/strength_map.csv")

    return state

def run_branch_search(state):
    strength_map = pd.read_csv("./temp/strength_map.csv", index_col=0)
    branches = prediction.set_branches(strength_map)
    for idx in range(len(branches)):
        state["branches"][idx] = branches[idx]
        if all(elem in strength_map.index  for elem in state["branches"][idx]):
            print("Branch is already in strength map ", idx, branches[idx])
            continue
        else:
            state["to_append"].append(idx)

    return state




def run_poly_search(state):
    strength_map = pd.read_csv("./temp/strength_map.csv", index_col=0)
    search_res, leak_branch = prediction.set_polynomes(strength_map, state)
    search_res = geo.raw_epsg3844_to_wgs84(search_res[0], search_res[1])
    search_multi_res = prediction.multi_leak_case(strength_map, state, leak_branch)
    state["converged"] = True
    state["algorithm_res"] = {1:search_res, 2:search_multi_res}
    return state
