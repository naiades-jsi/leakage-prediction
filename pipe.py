from regex import F
import prediction as prediction
import pandas as pd
from src import geo_converter as geo
import logging

LOGGER = logging.getLogger(__name__)

# Find the node with the highest noise signal
def run_crawler(state):
    # Generate/load state
    if not state:
        state = {
            "iter": -1,
            "state": "idle",
            "current_positions": [],
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

    # Run crawler algorithm
    res = prediction.hill_crawlerv2(strength_map, state)
    state["state"] = "running"

    if type(res) == str: ## Peak has been found
        LOGGER.info(f"Peak found at {res}")
        state["iter"] += 1
        state["crawl_complete"] = True
        state["crawl_res"] = res
        return state

    elif type(res) == list: ## List of nodes yet to check
        LOGGER.info(f"To check {res}")
        state["iter"] += 1
        state["crawl_complete"] = False
        state["crawl_res"] = res

    state["node_list"] = strength_map.index.tolist()
    return state

# Generate node branches for each leakage neighbor
def run_branch_search(state):
    strength_map = pd.read_csv("./temp/strength_map.csv", index_col=0)
    branches = prediction.set_branches(strength_map)
    if len(branches[0]) < 2 or len(branches[1]) < 2:
        branches = prediction.set_branches(strength_map, threshold_distance=400)
    if len(branches[0]) < 2 or len(branches[1]) < 2:
        LOGGER.warning("Branches are scarce despite extending the threshold distance.")
    for idx in range(len(branches)):
        state["branches"][idx] = branches[idx]
        LOGGER.info(f"Branch {idx}: {branches[idx]}")
        if all(elem in strength_map.index  for elem in state["branches"][idx]):
            LOGGER.info(f"Branch is already in strength map {idx, branches[idx]}")
            continue
        else:
            state["to_append"].append(idx)
    state["node_list"] = strength_map.index.tolist()
    return state

# Final algorithm for finding a leakage
def run_poly_search(state):
    failsafe = False
    # Adding a failsafe incase final step fails
    for idx in state["branches"].keys():
        if len(state["branches"][idx]) < 2:
            failsafe = True
    if failsafe:
        LOGGER.error("Branches were unable to complete, starting failsafe.") #this shouldn't be in the final product
        state = prediction.failsafe(state)
    strength_map = pd.read_csv("./temp/strength_map.csv", index_col=0)
    # Find leakage for single leakage case
    search_res, leak_branch = prediction.set_polynomes(strength_map, state)
    search_res = geo.raw_epsg3844_to_wgs84(search_res[0], search_res[1])
    # Find leakage for multiple leakage case
    search_multi_res = prediction.multi_leak_case(strength_map, state, leak_branch)
    state["converged"] = True
    state["algorithm_res"] = {1:search_res, 2:search_multi_res}
    state["state"] = "idle"
    return state
