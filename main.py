## make state inside temp
## make logs of relocations
import config
from processing import IO, geo_converter as geo, info_manager as mng
from time import sleep
import os.path
import pipe
import json




runLoop = False
testing = True
day = 86400 # number of seconds in a day - perform daily check

if os.path.exists("./temp/state.json"):
    print("Checking state...")
    f = open('./temp/state.json',)
    state = json.load(f)
    f.close()
else: 
    print("State not yet established.")
    state = None
    IO.runStart()
    mng.write_logs("State not yet established, starting a fresh run...")
    geo.conversion_table(config.layout_file_path, "./storage")
    mng.write_logs(f'Excecuting crawler')
    state = pipe.run_crawler(state)
    mng.write_logs(f'Crawler iteration complete. Nodes to check: {state["crawl_res"]}.')
    mng.write_state(state)
    IO.write_instructions(state["crawl_res"], is_start=True)
    mng.write_logs("Run completed successfully! Waiting for relocation.")
    

while True:
    is_moved, location, value = IO.read_instructions()


    if state and not state["crawl_complete"] and is_moved:
        mng.write_logs("Sensors have been relocated, starting calibration...")
        state = pipe.run_crawler(state)
        if state["crawl_complete"] == True:
            mng.write_logs(f'Crawling complete. Peak found at: {state["crawl_res"]}.')
            IO.write_instructions(state["crawl_res"])
        else: 
            mng.write_logs(f'Crawler iteration complete. Nodes to check: {state["crawl_res"]}.')
            IO.write_instructions(state["crawl_res"])
        mng.write_state(state) # adding checkpoint in case of error
        mng.write_logs("Run completed successfully! Waiting for relocation.")

    elif state and state["crawl_complete"] and len(state["to_append"]) != 0 and is_moved:  
        idx_of_branch = state["to_append"][0]
        IO.write_instructions(state["branches"][str(idx_of_branch)])
        state["to_append"].pop(0)
        mng.write_state(state)

    elif state and state["crawl_complete"] and len(state["branches"]) == 0:
        state = pipe.run_branch_search(state)
        idx_of_branch = state["to_append"][0]
        IO.write_instructions(state["branches"][str(idx_of_branch)])
        state["to_append"].pop(0)
        mng.write_state(state)

    elif state and state["crawl_complete"] and len(state["branches"]) != 0 and len(state["to_append"]) == 0: 
        state = pipe.run_poly_search(state)
        mng.write_state(state)
        IO.write_instructions(state["algorithm_res"], is_final=True)
        mng.write_logs(f'Program finished. Exit code 0. Leakage is at: {state["algorithm_res"]}')
        break


    else:
        print("Waiting for relocation.")
        mng.write_logs("Waiting for relocation.")
        sleep(day) 




