## make state inside temp
## make logs of relocations
import sys
from re import A
import logging
import config
from src import IO, geo_converter as geo
from time import sleep
import os.path
import pipe
import json
from optparse import OptionParser

# logging
LOGGER = logging.getLogger("accurate-leakage")
logging.basicConfig(
    format="%(asctime)s %(name)-16s %(levelname)-8s %(message)s", level=logging.INFO)

## parser
parser = OptionParser()
parser.add_option("-t", "--test", help="Setting this to will discard all new results upon completion. For debugging purposes.", action="store_true")
parser.add_option("-r", "--restart", help="Delete temporary files and start a fresh run.", action="store_true")
parser.add_option("-c", "--clear", help="Clears temporary files and exits.", action="store_true")
parser.add_option("-x", "--execute", help="Run by overriding the trigger argument.", action="store_true")
(options, args) = parser.parse_args()

## file memory in case of testing/debugging
import shutil
def copytree(src, dst, symlinks=False, ignore=None):
    for item in os.listdir(src):
        s = os.path.join(src, item)
        d = os.path.join(dst, item)
        if os.path.isdir(s):
            shutil.copytree(s, d, symlinks, ignore)
        else:
            shutil.copy2(s, d)
try:
    shutil.rmtree("testing_storage/")
except FileNotFoundError:
    pass

if options.test:
    os.mkdir("testing_storage/")
    copytree("temp/", "testing_storage/")

if options.restart:
    shutil.rmtree('temp/')
    os.mkdir("temp/")

if options.clear:
    shutil.rmtree('temp/')
    os.mkdir("temp/")
    exit()

run_loop = False
testing = False
check_interval = 8* 3600 # perform a check every 8 hours
IO.check_accessible_nodes_kafka()

while not run_loop:
    run_loop, values = IO.check_trigger()
    if options.execute: run_loop = True
    if run_loop:
        LOGGER.info("Trigger set to True, waiting 10 seconds")
        if not options.test: IO.send_trigger_off()
        sleep(10)
    else:
        sleep(3600)

# Checking if state already exists
if os.path.exists("./temp/state.json"):
    LOGGER.info("Checking state.")
    f = open('./temp/state.json')
    state = json.load(f)
    f.close()
else:
    state = None
    # Get current positions of each noise sensor and create temp/noise_sensors.csv
    current_positions = IO.runStart_kafka()
    LOGGER.info("State not created. Starting a fresh run.")
    LOGGER.info(f'Excecuting crawler, iteration: 0')
    state = pipe.run_crawler(state)
    state["current_positions"] = current_positions
    LOGGER.info(f'Crawler iteration complete. Nodes to check: {state["crawl_res"]}.')
    IO.write_state(state)
    
    # Signal back first result
    if not options.test:
        IO.write_instructions_kafka(state["crawl_res"])
    else:
        LOGGER.info("Testing mode enabled, data will not be sent.")
    LOGGER.info("Run completed successfully! Waiting for relocation. Time before next run: {}".format(check_interval))
    sleep(check_interval)


while True:
    # Check for changes in accessible nodes
    IO.update_node_list(state)
    # Read for sensor relocations
    is_moved, current_positions = IO.read_kafka(state)

    # Sensors have been relocated - step #1
    if state and not state["crawl_complete"] and is_moved:
        LOGGER.info("Sensors have been relocated.")
        state = pipe.run_crawler(state)
        if state["crawl_complete"] == True:
            LOGGER.info(f'Crawling complete. Peak found at: {state["crawl_res"]}.')
            if not options.test:
                IO.write_instructions_kafka(state["crawl_res"])
            else:
                LOGGER.info("Testing mode enabled, data will not be sent.")
        else:
            LOGGER.info(f'Crawler iteration complete. Nodes to check: {state["crawl_res"]}.')
            if not options.test:
                IO.write_instructions_kafka(state["crawl_res"])
            else:
                LOGGER.info("Testing mode enabled, data will not be sent.")
        # adding checkpoint in case of error
        IO.write_state(state)
        LOGGER.info(f"Run {state['iter']} completed successfully! Waiting for relocation.")
        

    # Commit every branch back for relocation - step #3
    elif state and state["crawl_complete"] and len(state["to_append"]) != 0 and is_moved:
        state["to_append"].pop(0)
        if len(state["to_append"]) == 0:
            
            continue
        idx_of_branch = state["to_append"][0]
        try:
            if not options.test:
                IO.write_instructions_kafka(state["branches"][idx_of_branch])
            else:
                LOGGER.info("Testing mode enabled, data will not be sent.")
        except KeyError as e:
            if not options.test:
                IO.write_instructions_kafka(state["branches"][str(idx_of_branch)])
            else:
                LOGGER.info("Testing mode enabled, data will not be sent.")
        IO.write_state(state)

    # Set branches and commit first branch - step #2
    elif state and state["crawl_complete"] and len(state["branches"]) == 0:
        state = pipe.run_branch_search(state)
        if len(state["to_append"]) == 0:
            
            continue
        idx_of_branch = state["to_append"][0]
        if not options.test:
            IO.write_instructions_kafka(state["branches"][idx_of_branch])
        else:
            LOGGER.info("Testing mode enabled, data will not be sent.")
        IO.write_state(state)

    # Run analysis on the system and find leakage - step #4
    elif state and state["crawl_complete"] and len(state["branches"]) != 0 and len(state["to_append"]) == 0:
        state = pipe.run_poly_search(state)
        
        if not options.test:
            IO.write_instructions_kafka(state["algorithm_res"], is_final=True)
            IO.send_final_location(state["algorithm_res"])
        else:
            LOGGER.info("Testing mode enabled, data will not be sent.")
        LOGGER.info(f'Program finished. Exit code 0. Leakage is at: {state["algorithm_res"]}')
        IO.write_state(state)
        exit()


    else:
        # Sleep if no new changes
        LOGGER.info("No sensor changes. Waiting for relocation.")
        # Remove information from last run.
        if options.test:
            LOGGER.info("Test mode was enabled, discarding last run.")
            copytree("testing_storage/", "temp/")
            shutil.rmtree("testing_storage/", ignore_errors=True)
            exit()

        sleep(check_interval)