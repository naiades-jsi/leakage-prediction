begin = True
kafka_bootstrap_server = "188.166.121.54:9092"
layout_DIR = "./layout/"
layout_file_path = "./layout/clean_network.json"
noise_data_DIR = "../dump/" 
kafka_params = {"bootstrap_servers": [kafka_bootstrap_server],
                "auto_offset_reset": "latest",
                "enable_auto_commit": "True",
                "group_id": "braila_noise_predictions",
                "value_deserializer": "lambda x: loads(x.decode('utf-8'))"}

noise_sensor_ids = ["2182", "5980", "5981", "5982"]
kafka_input_topic_prefix = "measurements_node_braila_noise"
kafka_output_topic_prefix =  "braila_leakage_position"

fiware_ip = "naiades.simavi.ro"  #"5.53.108.182"
fiware_port = "1026"
fiware_trigger_alert_id = "urn:ngsi-ld:Alert:RO-Braila-search-new-locations"
fiware_add_del_nodes_alert_id = "urn:ngsi-ld:Alert:RO-Braila-add-exclude-nodes"