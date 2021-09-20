import requests
import os
from pprint import pprint
import json
from paho.mqtt import client as mqtt_client
import random
import time
from datetime import datetime, timedelta

accessToken = os.environ['ACCESS_TOKEN']
broker = os.environ['MQTT_HOST']
port = int(os.environ['MQTT_PORT'])
prefix = os.environ['MQTT_PREFIX']
client_id = os.environ['MQTT_CLIENT_ID']
username = os.environ['MQTT_USERNAME']
password = os.environ['MQTT_PASSWORD']
cycle = int(os.environ['CYCLE'])

# Fix min cycle
if cycle < 10:
    cycle = 10

headers = {
    'accept': 'application/json',
    'Content-Type': 'application/json',
    'Authorization': 'Bearer ' + accessToken
}

endpoint = "https://jsonrpc.barry.energy/json-rpc"

def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    # Set Connecting Client ID
    client = mqtt_client.Client(client_id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def publish(client, topic, msg):
    msg_count = 0
    result = client.publish(f'{prefix}/{topic}', msg)
    status = result[0]
    if status == 0:
        print(f"Send `{msg}` to topic `{prefix}/{topic}`")
    else:
        print(f"Failed to send message to topic {prefix}/{topic}")
    msg_count += 1


def getPrice(priceCode, dateBegin=None, dateEnded=None):
    tag = "get-spot-price"
    print(f"=> {tag}")
    if dateBegin == None or dateEnded == None:
        my_date = datetime.now()
        dateBegin = my_date.strftime('%Y-%m-%dT%H:00:00Z')
        dateEnded = datetime.now() + timedelta(hours=1)
        dateEnded = dateEnded.strftime('%Y-%m-%dT%H:00:00Z')
    data = {
        "method": 'co.getbarry.api.v1.OpenApiController.getPrice',
        "id": 0,
        "jsonrpc": "2.0",
        "params": [
            priceCode,
            dateBegin,
            dateEnded
        ]
    }
    return requests.request("POST", url=f"{endpoint}#{tag}", headers=headers, data=json.dumps(data)).json()


def getPriceDefinitions():
    tag = "get-price-definitions"
    print(f"=> {tag}")
    data = {
        "method": 'co.getbarry.api.v1.OpenApiController.getPriceDefinitions',
        "id": 0,
        "jsonrpc": "2.0",
        "params": []
    }
    return requests.request("POST", url=f"{endpoint}#{tag}", headers=headers, data=json.dumps(data)).json()

def getMeteringPoints():
    tag = "get-metering-points"
    print(f"=> {tag}")
    data = {
        "method": 'co.getbarry.api.v1.OpenApiController.getMeteringPoints',
        "id": 0,
        "jsonrpc": "2.0",
        "params": []
    }
    return requests.request("POST", url=f"{endpoint}#{tag}", headers=headers, data=json.dumps(data)).json()

def getHourlyCo2Intensity(dateBegin=None, dateEnded=None):
    tag = "get-hourly-co2-emission"
    print(f"=> {tag}")
    if dateBegin == None or dateEnded == None:
        my_date = datetime.now()
        dateBegin = my_date.strftime('%Y-%m-%dT%H:00:00Z')
        dateEnded = datetime.now() + timedelta(hours=1)
        dateEnded = dateEnded.strftime('%Y-%m-%dT%H:00:00Z')
    data = {
        "method": 'co.getbarry.api.v1.OpenApiController.getHourlyCo2Intensity',
        "id": 0,
        "jsonrpc": "2.0",
        "params": [
            dateBegin,
            dateEnded
        ]
    }
    return requests.request("POST", url=f"{endpoint}#{tag}", headers=headers, data=json.dumps(data)).json()


def run():
    client = connect_mqtt()
    client.loop_start()
    while True:

        # GET CONTRACT INFORMATION
        gmp = getMeteringPoints()
        for data in gmp['result']:
            pdl = data['mpid']
            priceCode = data['priceCode']
            for key, value in data.items():
                if key == "address":
                    for address_key, address_value in value.items():
                        publish(client, f"{pdl}/address/{address_key}", str(address_value))
                if key != "mpid":
                    publish(client, f"{pdl}/{key}", str(value))
            # pprint(gmp)

            gsp = getPrice(priceCode)
            for result in gsp['result']:
                value = result['value']
                publish(client, f"{pdl}/currentPrice", str(value))

            # gpd = getPriceDefinitions()
            # publish(client, "get-price-definitions", str(gpd))
            # pprint(gpd)

        # ghco2 = getHourlyCo2Intensity()
        # pprint(ghco2)

        time.sleep(cycle)


if __name__ == '__main__':
    run()
