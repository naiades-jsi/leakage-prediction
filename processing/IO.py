### IO module for retrieving and sending sensor relocation instructions
from numpy.lib.function_base import _DIMENSION_NAME
import config
import json
import pandas as pd
import os
import processing.geo_converter as geo_converter
from csv import writer
from kafka import KafkaProducer, KafkaConsumer
import time 

config_DIR = "./config/"
dump_folder_DIR = config.noise_data_DIR

def create_holder(config_DIR):
    f = open(f'{config_DIR}output_form.json',)
    holder = json.load(f)
    f.close()
    return holder


def save_instructions(): # saves retrieved data from api to /dump/log 

    return



def runStart():
    ## change starting sensors
    f = open('./config/init_param.json',)
    dir_files = os.listdir(dump_folder_DIR)
    init_param = json.load(f)
    f.close()
    starting_sensors = []
    noise_sensors = {}
    i= 0
    loudest_noise = 0
    for sensor in [x for x in dir_files if "braila_noise" in x]:
        sensor_data = pd.read_csv(f"{dump_folder_DIR}{sensor}")
        try:
            last_entry = sensor_data.iloc[-1]
            parsed = last_entry.to_numpy()
            location = eval(parsed[5])
            junction, x, y = geo_converter.wgs84_to_3844(location["coordinates"][0], location["coordinates"][1])
            starting_sensors.append(junction)
            noise_sensors[i] = [sensor, "name", location["coordinates"][0], location["coordinates"][1], junction, parsed[2]]
            i += 1
            if loudest_noise < float(parsed[2]):
                loudest_noise = float(parsed[2])
            
        except Exception as e:
            print(e)

    init_param["starting_sensors"] = starting_sensors
    init_param["source_strength"]  = loudest_noise
    with open("./config/init_param.json", "w") as outfile: 
            json.dump(init_param, outfile)
    noise_df = pd.DataFrame.from_dict(noise_sensors, columns=["ENTITY ID","LOCATION","LONGITUDE","LATITUDE","INP NAME","READING"], orient="index")
    noise_df.to_csv("./temp/noise_sensors.csv")

def runStart_kafka():
    ## change starting sensors
    noise_sensors = {}
    i= 0
    consumer = KafkaConsumer(bootstrap_servers= ["192.168.2.100:9092"],
                auto_offset_reset= "latest",
                enable_auto_commit= True,
                auto_commit_interval_ms=2000,
                group_id= "braila_noise_predictions",
                value_deserializer= lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=3000

                )
    for sensor_id in config.noise_sensor_ids:
        sensor = "braila_noise"+sensor_id
        consumer.subscribe([sensor])
        
        last_entry = None
        for message in consumer:
            last_entry = message.value
         
        location = eval(last_entry["location"])
        junction, x, y = geo_converter.wgs84_to_3844(location["coordinates"][0], location["coordinates"][1])
        noise_sensors[i] = [sensor, "name", location["coordinates"][0], location["coordinates"][1], junction, last_entry["noise_db"]]
        i += 1
            

    noise_df = pd.DataFrame.from_dict(noise_sensors, columns=["ENTITY ID","LOCATION","LONGITUDE","LATITUDE","INP NAME","READING"], orient="index")
    noise_df.to_csv("./temp/noise_sensors.csv")



def send_to_kafka(sensor, data, is_final):
    producer = KafkaProducer(bootstrap_servers=["192.168.2.100:9092"])
    topic = f"braila_leakage_position{sensor[-4:]}"
    if is_final: 
        topic = "braila_leakage_position2182"
        to_send = json.dumps({
            "timestamp": int(time.time()),
            "position": data["location"]["coordinates"], 
            "final_location" : True
        })
    else: 
        to_send = json.dumps({
            "timestamp": int(time.time()),
            "position": data, ## data is [x, y]
            "final_location" : False
        })
    producer.send(topic, to_send.encode("utf-8"))
    return

def read_instructions(): ## add to read from api
    dir_files = os.listdir(dump_folder_DIR)
    for sensor in [x for x in dir_files if "braila_noise" in x]:
        sensor_data = pd.read_csv(f"{dump_folder_DIR}{sensor}")
        try:
            last_entry = sensor_data.iloc[-1]
            if last_entry[["isMovedToNewLocation"]].all():
                parsed = last_entry.to_numpy()
                location = eval(parsed[5])
                junction, x, y = geo_converter.wgs84_to_3844(location["coordinates"][0], location["coordinates"][1])
                save_instructions()
                noise_df = pd.read_csv("./temp/noise_sensors.csv", index_col=0)
                noise_df.loc[len(noise_df.index)] = [sensor, "name", location["coordinates"][0], location["coordinates"][1], junction, parsed[2]]
                noise_df.to_csv("./temp/noise_sensors.csv")
                strength_map = pd.read_csv("./temp/strength_map.csv", index_col=0)
                #print(x)
                strength_map.loc[junction] = [x.item(), y.item(), last_entry["noise_db"]]
                strength_map.to_csv("./temp/strength_map.csv")
                last_entry["isMovedToNewLocation"] = False
                with open(f"{dump_folder_DIR}{sensor}",'a+', newline='') as fd:
                    writer(fd).writerow(last_entry.to_numpy())


                return True, junction, parsed[2] ## parsed[2] and [5] are noise_db and location respectively
            else:
                continue
            
        except Exception as e:
            print(e, "-- In IO.read_instructions")
            
            return False, None, None
    return False, None, None
        


def write_instructions(node, is_final=False, is_start=False):
    if is_final:
        data = {"location" : {"coordinates" : node}, "is_final": True}
#        send_to_kafka("2182", data, is_final=True) ### add kafka functionality
    else:
        n_instructions = len(node) if type(node) == list else 1
        dir_files = os.listdir(dump_folder_DIR)
        sensors = ["braila_noise2182", "braila_noise5980", "braila_noise5981", "braila_noise5982"]
        for node_idx in range(n_instructions):
            sensor = sensors[node_idx]
            sensor_data = pd.read_csv(f"{dump_folder_DIR}{sensor}.csv")
            last_entry = sensor_data.iloc[-1].copy(deep=True)

            if n_instructions > 1:
                junction, x_WGS84, y_WGS84, x, y = geo_converter.get_geo_info(node[node_idx])
            elif n_instructions == 1:
                junction, x_WGS84, y_WGS84, x, y = geo_converter.get_geo_info(node)
            if True:
                last_entry["location"] = {'coordinates': [x_WGS84, y_WGS84], 'type': 'Point'}
                last_entry["time"] = int(time.time())
                last_entry["isMovedToNewLocation"] = False
                is_start = False
                with open(f"{dump_folder_DIR}{sensor}.csv",'a+', newline='') as fd:
                    writer(fd).writerow(last_entry.to_numpy())
#               send_to_kafka(sensor, last_entry, is_final=False) ### add kafka functionality
    return

def write_instructions_kafka(node, is_final=False, is_start=False):
    if is_final:
        data = {"location" : {"coordinates" : node}, "is_final": True}
        send_to_kafka("2182", data, is_final=True) 
    else:
        n_instructions = len(node) if type(node) == list else 1
        sensors = ["braila_noise2182", "braila_noise5980", "braila_noise5981", "braila_noise5982"]
        for node_idx in range(n_instructions):
            sensor = sensors[node_idx]

            if n_instructions > 1:
                junction, x_WGS84, y_WGS84, x, y = geo_converter.get_geo_info(node[node_idx])
            elif n_instructions == 1:
                junction, x_WGS84, y_WGS84, x, y = geo_converter.get_geo_info(node)
            
            send_to_kafka(sensor, [x_WGS84, y_WGS84], is_final=False) ### add kafka functionality
    return

def read_kafka(): ## read from kafka input topic
    moved_sensors = {}
    consumer = KafkaConsumer(bootstrap_servers= ["192.168.2.100:9092"],
                auto_offset_reset= "latest",
                enable_auto_commit= True,
                auto_commit_interval_ms=2000,
                group_id= "braila_noise_predictions",
                value_deserializer= lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms= 3000
                )
    for sensor_id in config.noise_sensor_ids:
        sensor = "braila_noise"+sensor_id
        
        consumer.subscribe([sensor])
        last_entry = None
        for message in consumer:
            last_entry = message.value
            print(sensor, last_entry)
        if last_entry and last_entry["isMovedToNewLocation"]:
            moved_sensors[sensor] = last_entry
        
    for sensor in moved_sensors:
        try:
            location = eval(moved_sensors[sensor]["location"])
            junction, x, y = geo_converter.wgs84_to_3844(location["coordinates"][0], location["coordinates"][1])
            noise_df = pd.read_csv("./temp/noise_sensors.csv", index_col=0)
            noise_df.loc[len(noise_df.index)] = [sensor, "name", location["coordinates"][0], location["coordinates"][1], junction, moved_sensors[sensor]["noise_db"]] ## and this
            noise_df.to_csv("./temp/noise_sensors.csv")
            strength_map = pd.read_csv("./temp/strength_map.csv", index_col=0)
            strength_map.loc[junction] = [x.item(), y.item(), moved_sensors[sensor]["noise_db"]] ### this too
            strength_map.to_csv("./temp/strength_map.csv")
    
        
        except Exception as e:
            print(e, "-- In IO.read_kafka")
            return False, None, None
    print(moved_sensors.keys())
    if len(moved_sensors.keys()) > 0:
        return True, "junction", "parsed[2]" ## parsed[2] and [5] are noise_db and location respectively ------- DONT NEED LAST 2 ANYWAY
    
    else:
        return False, None, None

    