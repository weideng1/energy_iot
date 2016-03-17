#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import random
import datetime
from time import sleep
import csv
import sys


def generate_sensor_data(num_sensors):
    time = datetime.datetime.now()
    epochtime = int(time.strftime("%s"))
    start_time = epochtime - 86400
    results = []
    for device in range(1,num_sensors+1):
        start_time = epochtime - 86400
        for j in range(1,97):
            results.append([str(device), datetime.datetime.fromtimestamp(start_time), 'KWH', random.uniform(0.1, 1.9)])
            start_time = start_time + 900
    return results

def main():
    results = generate_sensor_data(int(sys.argv[1]))
    b = open('sensordata'+str(datetime.datetime.now())+'.csv', 'w')
    a = csv.writer(b)
    for result in results:
        a.writerow(result)
    b.close
    print sys.argv[1]+' sensors data generated'

if __name__ == "__main__":
    main()
