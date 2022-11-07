### IO module for retrieving and sending sensor relocation instructions
from email import contentmanager
from multiprocessing import current_process
from re import A
from numpy.lib.function_base import _DIMENSION_NAME
from sympy import prime
import config
import json
import pandas as pd
import os
import src.geo_converter as geo_converter
from csv import writer
from kafka import KafkaProducer, KafkaConsumer
import time
from typing import Any, Dict, List, Optional
import requests
import logging

args = None

LOGGER = logging.getLogger(__name__)
logging.getLogger("kafka").setLevel(logging.CRITICAL)

config_DIR = "./config/"
dump_folder_DIR = config.noise_data_DIR
bootstrap_server = config.kafka_bootstrap_server
# This should be changet to fit the needs of the download (to alert)
ip = config.fiware_ip
port = config.fiware_port

def write_state(state):
    with open("./temp/state.json", "w") as outfile:
            json.dump(state, outfile)

def create_holder(config_DIR):
    f = open(f'{config_DIR}output_form.json',)
    holder = json.load(f)
    f.close()
    return holder

def get_content(entity_id) -> Dict[str, Any]:
    # Send the get request
    base_url = "http://" + ip + ":" + port + "/v2/entities/" + entity_id
    headers = {
    "fiware-Service": "braila",
    "fiware-ServicePath": "/"
    }
    try:
        r = requests.get(base_url, headers=headers)
    except requests.exceptions.RequestException as e:  # This is the correct syntax
        LOGGER.warning(e)
    else:
        LOGGER.info('Successfuly obtained response from API ' + time.ctime())

        # If status code is not 200 raise an error
        if(r.status_code != requests.codes.ok):
            print("Data could not be obtained. Error code: {}.".format( r.status_code))
            return

        body = r.json()

        return body

def send_content(entity_id, content):
    # Send the get request
    base_url = "http://" + ip + ":" + port + "/ngsi-ld/v1/entities/" + entity_id + "/attrs/description"
    headers = {
    "fiware-Service": "braila",
    "Content-Type": "application/json"
    }
    LOGGER.info(f"Sending data to {base_url}")
    try:
        # Content is type JSON
        r = requests.patch(base_url, data=json.dumps(content), headers=headers)
    except requests.exceptions.RequestException as e:  # This is the correct syntax
        LOGGER.warning(e)
    else:
        LOGGER.info('Successfuly sent to API ' + time.ctime())

        # If status code is not 200 raise an error
        if(r.status_code != requests.codes.ok and r.status_code != requests.codes.no_content):
            LOGGER.error("Data could not be sent. Error code: {}.".format( r.status_code))
            return
    return

def send_trigger_off():
    entity_id = config.fiware_trigger_alert_id
    content = {
        "value": "False",
        "type": "Property"
    }
    LOGGER.info("Setting trigger to false.")
    send_content(entity_id, content)
    return

def send_final_location(data):
    entity_id = config.fiware_add_del_nodes_alert_id
    content = {
        "type": "Property",
        "value": str(data)
    }
    send_content(entity_id, content)
    return

def runStart_kafka():
    noise_sensors = {}
    current_nodes = []
    i= 0
    # Retrieve messages on kafka
    consumer = KafkaConsumer(bootstrap_servers= [bootstrap_server],
                auto_offset_reset= "earliest",
                enable_auto_commit= False,
                group_id= None,
                value_deserializer= lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=3000

                )
    for sensor_id in config.noise_sensor_ids:
        sensor = config.kafka_input_topic_prefix+sensor_id
        consumer.subscribe([sensor])

        last_entry = None
        last_valid = None
        for message in consumer:
            last_entry = message.value
            if not last_entry["noise_db"]:
                LOGGER.info(f"{sensor_id}, Noise was null. Checking previous entry")
                last_entry = last_valid
            else:
                last_valid = message.value
        LOGGER.info(f"Last entry for {sensor}: {last_entry}")
        if type(last_entry["location"]) != dict:
            location = eval(last_entry["location"])
        else:
            location = last_entry["location"]
        junction, x, y = geo_converter.wgs84_to_3844(location["coordinates"][0], location["coordinates"][1])
        current_nodes.append(junction)
        noise_sensors[i] = [sensor, "name", location["coordinates"][0], location["coordinates"][1], junction, last_entry["noise_db"]]
        i += 1

    # Create noise sensor list
    noise_df = pd.DataFrame.from_dict(noise_sensors, columns=["ENTITY ID","LOCATION","LONGITUDE","LATITUDE","INP NAME","READING"], orient="index")
    noise_df.to_csv("./temp/noise_sensors.csv")
    return current_nodes


def send_to_kafka(sensor, data, is_final):
    producer = KafkaProducer(bootstrap_servers=[bootstrap_server])
    topic = f"{config.kafka_output_topic_prefix}{sensor[-4:]}"
    LOGGER.info(f"Sending data to kafka for sensor: {sensor}. Data: {data}")
    # This form if for intermediate results - not for final result
    to_send = json.dumps({
        "timestamp": int(time.time()),
        "position": data, # data is [x, y]
        "is_final" : False
    })
    if args.test: LOGGER.info("Testing mode enabled, data will not be sent.")
    else:
        producer.send(topic, to_send.encode("utf-8"))
    return

def send_alert(data, is_final=False):
    producer = KafkaProducer(bootstrap_servers=[bootstrap_server])
    if is_final:
        topic = "braila_noise_final"
        to_send = json.dumps({
            "timestamp": int(time.time()),
            "position": data["location"]["coordinates"],
            "final_location" : True
            })
        if args.test:
            LOGGER.info("Test mode enabled, final location will not be forwarded to API.")
        else:
            producer.send(topic, to_send.encode("utf-8"))
    return

def write_instructions_kafka(node, is_final=False):
    if is_final:
        data = {"location" : {"coordinates" : node}, "is_final": True}
        #send_to_kafka("2182", data, is_final=True) - not in use
        send_alert(data, is_final=True)
    else:
        n_instructions = len(node) if type(node) == list else 1
        sensors = [config.kafka_input_topic_prefix+x for x in config.noise_sensor_ids]
        for node_idx in range(n_instructions):
            sensor = sensors[node_idx]

            if n_instructions > 1:
                junction, x_WGS84, y_WGS84, x, y = geo_converter.get_geo_info(node[node_idx])
            elif n_instructions == 1:
                junction, x_WGS84, y_WGS84, x, y = geo_converter.get_geo_info(node)

            try:
                send_to_kafka(sensor, [x_WGS84.values[0], y_WGS84.values[0]], is_final=False)
            except AttributeError:
                send_to_kafka(sensor, [x_WGS84, y_WGS84], is_final=False)
    return

def read_kafka(state):
    moved_sensors = {}

    ##checking state for new sensor locations to compare
    node_list = state["current_positions"]

    consumer = KafkaConsumer(bootstrap_servers= [bootstrap_server],
                auto_offset_reset= "latest",
                enable_auto_commit= True,
                auto_commit_interval_ms=2000,
                group_id= "braila_noise_predictions",
                value_deserializer= lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms= 3000
                )
    for sensor_id in config.noise_sensor_ids:
        sensor = config.kafka_input_topic_prefix+sensor_id

        consumer.subscribe([sensor])
        last_entry = None
        for message in consumer:
            last_entry = message.value
            LOGGER.info(f"{sensor, last_entry}")
        if last_entry:
            moved_sensors[sensor] = last_entry

    current_positions = []
    n_moved = 0

    for sensor in moved_sensors: ## "moved sensor" is not necessarily moved irl
        try:
            location = eval(moved_sensors[sensor]["location"])
            newLocation = eval(moved_sensors[sensor]["isMovedToNewLocation"])
            junction, x, y = geo_converter.wgs84_to_3844(location["coordinates"][0], location["coordinates"][1])
            current_positions.append(junction)

            if junction in node_list: ## code below is for sensors that have been moved
                LOGGER.info(f"{junction} already in node_list")
                continue
            if location["coordinates"][0] != newLocation["coordinates"][0]:
                LOGGER.info(f"Sensor {sensor} didn't move.")
                continue

            noise_df = pd.read_csv("./temp/noise_sensors.csv", index_col=0)
            noise_df.loc[len(noise_df.index)] = [sensor, "name", location["coordinates"][0], location["coordinates"][1], junction, moved_sensors[sensor]["noise_db"]]
            noise_df.to_csv("./temp/noise_sensors.csv")
            strength_map = pd.read_csv("./temp/strength_map.csv", index_col=0)
            strength_map.loc[junction] = [x.item(), y.item(), moved_sensors[sensor]["noise_db"]]
            strength_map.to_csv("./temp/strength_map.csv")
            n_moved += 1

        except Exception as e:
            LOGGER.info(f"{e}, --IO.read_kafka")
            LOGGER.info(f"Tried for {moved_sensors[sensor]}")
            return False, None
    if n_moved > 0:
        LOGGER.info("N moved is more than 0 {n_moved}")
        return True, current_positions

    else:
        return False, current_positions

def check_accessible_nodes_kafka(path="./layout/accessible_nodes.txt"):
    node_list_raw = get_content(config.fiware_add_del_nodes_alert_id)["https://uri.etsi.org/ngsi-ld/data"]["value"]
    parsed_nodes = []
    for id in node_list_raw.keys():
        if "id" in id:
            node_name = f"Jonctiune-{id[3:]}"
        else:
            node_name = f"Jonctiune-{id}"

        if node_list_raw[id][-1] == "exclude" or node_list_raw[id][0] == None or node_list_raw[id][1] == None:

            pass

        elif node_list_raw[id][-1] == "add" or node_list_raw[id][-1] == None:
            parsed_nodes.append(node_name)

        else:
            LOGGER.info("While checking node accessibility, node with id --{}-- had unclear instructions.".format(id))

    with open(path, "w") as outfile:
        outfile.write("\n".join(str(item) for item in parsed_nodes))

    return

def update_node_list(state, path="./layout/accessible_nodes.txt"):
    changed = check_accessible_nodes_kafka()
    return

def check_trigger():
    LOGGER.info("Checking trigger.")
    res = get_content(config.fiware_trigger_alert_id)
    triggered = res['https://uri.etsi.org/ngsi-ld/description']["value"]
    triggered = True if triggered == "true" or triggered == "True" else False
    sensor_positions = res["https://uri.etsi.org/ngsi-ld/data"]["value"]
    return triggered, sensor_positions
