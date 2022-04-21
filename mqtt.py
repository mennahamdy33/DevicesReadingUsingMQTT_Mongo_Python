from paho.mqtt import client as mqtt_client
import pymongo
import json

broker = 'broker.emqx.io'
port = 1883
topic = "python/mqtt"


def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client("Python Code")
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        c = pymongo.MongoClient('mongodb+srv://Iot_P1_MZ:52248AtlasP1@iotproject1cluster.swr9t.mongodb.net/myFirstDatabase?retryWrites=true&w=majority')
        db = c['ITI']
        collection = db.Embedded
        x = json.loads(msg.payload.decode())
        if 'msg' not in x:
            print("the object: ",x)
            collection.insert_one(x).inserted_id
    client.subscribe(topic)
    client.on_message = on_message


def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()



if __name__ == '__main__':
    run()