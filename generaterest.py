import requests
import time
import random
import datetime
from time import sleep
import sys
import json

def generate_sensor_data(num_sensors):
    time = datetime.datetime.now()
    epochtime = int(time.strftime("%s"))
    start_time = epochtime - 86400
    readings = []
    for device in range(1,num_sensors+1):
        start_time = epochtime - 86400
        for j in range(1,97):
            readings.append([str(device), str(start_time), 'KWH', str(random.uniform(0.1, 1.9))])
            start_time = start_time + 900
    return readings

def create_request(reading):
    #reading_string = '{"records": [ {"value":"' + reading[0] +','+ reading[1] +','+ reading[2] +','+ reading[3] + '" }]}'
    reading_string = {"value_schema": "{\"type\": \"record\", \"name\": \"Reading\", \"fields\": [{\"name\": \"device_id\", \"type\": \"string\"},{\"name\": \"metric_time\", \"type\": \"string\"},{\"name\": \"metric_name\", \"type\": \"string\"},{\"name\": \"metric_value\", \"type\": \"string\"}]}", "records": [{"value": {"device_id": reading[0],"metric_time": reading[1],"metric_name": reading[2],"metric_value": reading[3]}}]}
    reading_string = json.dumps(reading_string)
    return reading_string

def simulate_rest_writes(readings):
    url = 'http://localhost:8082/topics/testmeter'
    headers = {'Content-Type' : 'application/vnd.kafka.avro.v1+json'}
    for reading in readings:
        reading_payload = create_request(reading)
        response = requests.post(url,data=reading_payload,headers=headers)

        print reading_payload
        print response
        sleep(1)

def main():
    readings = generate_sensor_data(int(sys.argv[1]))
    simulate_rest_writes(readings)
    print 'complete'

if __name__ == "__main__":
    main()
