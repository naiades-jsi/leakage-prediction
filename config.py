begin = True
layout_DIR = "./layout/"
layout_file_path = "./layout/clean_network.json"
noise_data_DIR = "../dump/" 
kafka_params = {"bootstrap_servers": ["localhost:9092"],
                "auto_offset_reset": "latest",
                "enable_auto_commit": "True",
                "group_id": "braila_noise_predictions",
                "value_deserializer": "lambda x: loads(x.decode('utf-8'))"}
