# Optimized sensor movement manual* 
#### Headless for server use

## Modules
- IO - serves to pre- and postprocess information sent between the algorithm and CUP Braila
- Config - includes user-set algorithm parameters and file directories
- Pipe - serves as an adapter to prediction module - formats data and updates state
- Main - connects the whole pipeline together in a concise if/else manner - the program is run from here
- Prediction - module including everything related to algorithm (prediction, calibration, exit routine)
- Temp - includes files storing data from current prediction movement - sensors used, map of signals, *state* file
- Simulator - mainly used to test out new features and algorithm predictions

## IO
Read instructions - reads data from dump file on the server - path is specified in config file
Write instructions - parses new information gathered by the algorithm and sends back to Kafka
## ADD KAFKA FUNCTIONALITY **** implement final location parameter info in send_to_kafka()

Functions: 

- When starting a new search -> runStart() scrapes noise data dumps and creates /temp/noise_sensor.csv
- When a single sensor is relocated -> read_instructions() scrapes data dump for changed sensor and returns: is_moved boolean, node name of new junction, noise read by the relocated sensor.
- After prediction suggests a new location -> write_instructions() edits dump file to change is_moved to "false", then sends the same information to Kafka for CUP Braila to read.


## PIPE

### Consisting of two functions - run_predict() and run_calibrate() - it serves as an adater that binds the algorithm to the rest of the pipeline.

Functions: 

- run_predict() - sets and/or updates state based on prediction result. Returns new state back to main.py
- run_calibrate() - updates state with improved algorithm parameters and sends state back to main.py

## PREDICTION

### Currently running old version of algorithm - will update once finished.

#### Houses all necessary functions to construct a grid of vertices upon which the algorithm is run. 

- locate() - serves as the main function for prediction step. Looks trough all (4) sensors to find lowest calculation discrepancy, iteratively updates hypothetical strength of signal emitted by the leakage - then chooses the lowest value at which the sensor calculations are in best agreement (if all sensor calculations have low enough difference, the procedure is immedeately stopped to preserve execution time). 

- calibrate() - serves as the main function of calibration step. It compares data received by newly relocated sensor to previously predicted hypothetical source signal strengths. The parameters that serve in prediction step are then tweaked to insure better system accomodation and therefore better future predictions. 



## MAIN

#### As the name suggests, this is the main file of the entire package. It controls other - secondary pipelines to connect it in a functioning whole. Includes checking for new data/new relocations once a day. 
#### The function also sets up all needed files for further steps. 
Excecute whole system using *python main.py*.
