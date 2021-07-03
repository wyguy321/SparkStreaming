from kafka import KafkaProducer
from kafka import KafkaConsumer
import sys
import pandas
import pandas as pd
import os

bootstrap_servers = [XXXXX.c2.kafka.us-east-1.amazonaws.com:9092','XXXXXX.c2.kafka.us-east-1.amazonaws.com:9092']
topicName = 'example_topic'
producer = KafkaProducer(bootstrap_servers = bootstrap_servers)








for subdir, dirs, files in os.walk('/tempDIR'):
    for f in files:
        print(f)
        file = '/tempDIR/'+f
        print(file)
        df = pd.read_csv(file)
        print(df.head())
        for index, row in df.iterrows():
            ######QUESTIONS 2.1,2.2,2.3
            ##airline = str(row['Airline']+",")
            ##averageDelay = str(row['Average delay in minutes'])
            ##averageDelayMsg = averageDelay
           
            ##origin = str(row['Origin'])
            ##record=airline+averageDelayMsg+","+origin
            ##producer.send(topicName, key=b'RECORD', value=str.encode(record))

            ######QUESTION 3.2
            ##airline = str(row['Orgin']+",")
            ##averageDelay = str(row['Destination'])
            ##schedDepar = str(row['Scheld Depart'])
            ##arrDelay = str(row['Arrival delay'])
            ##record=airline+","+averageDelay+","+schedDepar+","+arrDelay
            ##producer.send(topicName, key=b'RECORD', value=str.encode(record))


