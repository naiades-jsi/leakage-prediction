## make state inside temp
## make logs of relocations
import sys
from re import A
import config
from processing import IO, geo_converter as geo, info_manager as mng
from time import sleep
import os.path
import pipe
import json
import argparse


## parser 
parser = argparse.ArgumentParser()
parser.add_argument('--test', type=str, help="Setting this to will discard all new results upon completion. For debugging purposes.")
parser.add_argument('--restart', type=str, help="Delete temporary files and start a fresh run.")
parser.add_argument('--clear', type=str, help="Clears temporary files and exits.")
args = parser.parse_args()
IO.args = args


## file perserver
import shutil
def copytree(src, dst, symlinks=False, ignore=None):
    for item in os.listdir(src):
        s = os.path.join(src, item)
        d = os.path.join(dst, item)
        if os.path.isdir(s):
            shutil.copytree(s, d, symlinks, ignore)
        else:
            shutil.copy2(s, d)

if args.test:
    os.mkdir("testing_storage/")
    copytree("temp/", "testing_storage/")

if args.restart:
    shutil.rmtree('temp/')
    os.mkdir("temp/")

if args.clear:
    shutil.rmtree('temp/')
    os.mkdir("temp/")
    exit()

run_loop = False
testing = False
day = 24* 3600# number of seconds in a day - perform daily check
IO.check_accessible_nodes_kafka()
while not run_loop:
    run_loop, values = IO.check_trigger()
    if run_loop:
        print("Trigger True, waiting 10 seconds")
        sleep(10)
    else:
        sleep(3600)
    

if os.path.exists("./temp/state.json"):
    print("Checking state...")
    f = open('./temp/state.json',)
    state = json.load(f)
    f.close()
else: 
    print("State not yet established.")
    state = None
    current_positions = IO.runStart_kafka()
    mng.write_logs("State not yet established, starting a fresh run...")
    #IO.update_node_list(state)
    mng.write_logs(f'Excecuting crawler')
    state = pipe.run_crawler(state)
    state["current_positions"] = current_positions
    mng.write_logs(f'Crawler iteration complete. Nodes to check: {state["crawl_res"]}.')
    mng.write_state(state)
    IO.write_instructions_kafka(state["crawl_res"], is_start=True)
    mng.write_logs("Run completed successfully! Waiting for relocation.")
    sleep(day)
    

while True:
    IO.update_node_list(state)
    is_moved, current_positions = IO.read_kafka(state)


    if state and not state["crawl_complete"] and is_moved:
        mng.write_logs("Sensors have been relocated, starting calibration...")
        state = pipe.run_crawler(state)
        if state["crawl_complete"] == True:
            mng.write_logs(f'Crawling complete. Peak found at: {state["crawl_res"]}.')
            IO.write_instructions_kafka(state["crawl_res"])
        else: 
            mng.write_logs(f'Crawler iteration complete. Nodes to check: {state["crawl_res"]}.')
            IO.write_instructions_kafka(state["crawl_res"])
        mng.write_state(state) # adding checkpoint in case of error
        mng.write_logs("Run completed successfully! Waiting for relocation.")

    elif state and state["crawl_complete"] and len(state["to_append"]) != 0 and is_moved:  
        state["to_append"].pop(0)
        if len(state["to_append"]) == 0: 
            mng.write_state(state)
            continue
        idx_of_branch = state["to_append"][0]
        try:
            IO.write_instructions_kafka(state["branches"][idx_of_branch])
        except KeyError as e:
            IO.write_instructions_kafka(state["branches"][str(idx_of_branch)])
        mng.write_state(state)

    elif state and state["crawl_complete"] and len(state["branches"]) == 0:
        state = pipe.run_branch_search(state)
        if len(state["to_append"]) == 0:
            mng.write_state(state)
            continue
        idx_of_branch = state["to_append"][0]
        IO.write_instructions_kafka(state["branches"][idx_of_branch])
        mng.write_state(state)

    elif state and state["crawl_complete"] and len(state["branches"]) != 0 and len(state["to_append"]) == 0: 
        state = pipe.run_poly_search(state)
        mng.write_state(state)
        IO.write_instructions_kafka(state["algorithm_res"], is_final=True)
        mng.write_logs(f'Program finished. Exit code 0. Leakage is at: {state["algorithm_res"]}')
        break


    else:
        print("Waiting for relocation.")
        mng.write_logs("Waiting for relocation.")
        if args.test:
            copytree("testing_storage/", "temp/")
            shutil.rmtree("testing_storage/", ignore_errors=True)
            raise SystemExit
            
        sleep(day)
           	



