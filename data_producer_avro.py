import datetime
import threading
from decimal import *
from time import sleep
from uuid import uuid4,UUID
import time 


from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from dotenv import load_dotenv
import os
import pandas as pd
load_dotenv()


def delivery_report(err,msg):
    if err is not None:
        print("Delivery failed for user record{}:{}".format(msg.key(),err))
        return
    print("User record{} successfully produced to {} [{}] at offset{}".format(msg.key(),msg.topic(),msg.partition(),msg.offset()))
    print("************************")

schema_registry_client=SchemaRegistryClient({
    'url': os.getenv('SCHEMA_REGISTRY_URL'),
    'basic.auth.user.info': f"{os.getenv('SCHEMA_REGISTRY_API_KEY')}:{os.getenv('SCHEMA_REGISTRY_API_SECRET')}",})



#Fetch the latest avro schema for the value 
subject_name='retail_data_dev-value'
schema_str=schema_registry_client.get_latest_version(subject_name).schema.schema_str
print("Schema from Registry")
print(schema_str)
print("***************")


#create Avro Serializer for the value
key_serializer=StringSerializer('utf-8')
avro_serializer=AvroSerializer(schema_registry_client,schema_str)

#Define the serializingproducer

producer=SerializingProducer (
    {
    'bootstrap.servers': os.getenv('BOOTSTRAP_SERVERS'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv('SASL_USERNAME'),
    'sasl.password': os.getenv('SASL_PASSWORD'),
    'key.serializer':key_serializer,
    'value.serializer':avro_serializer})


#load the csv data into a pandas Dataframe 

df=pd.read_csv("retail_data.csv")
df=df.fillna("null")

for index,row in df.iterrows():
    data_value=row.to_dict()
    print(data_value)
    producer.produce(
        topic='retail_data_dev',
        key=str(index),
        value=data_value,
        on_delivery=delivery_report
    )
    producer.flush()
    time.sleep(2)

print("All the data sucessfully published to kafka")






