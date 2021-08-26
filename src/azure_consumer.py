import azure.cosmos.documents as documents
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.exceptions as exceptions
from azure.cosmos.partition_key import PartitionKey
import datetime
import asyncio
import config
import cryptowatch as cw
import time
from google.protobuf.json_format import MessageToJson
import json
import uuid
from kafka import KafkaConsumer
from kafka.structs import TopicPartition


class AzureConsumer:
    def __init__(self, cont) -> None:
        self.HOST = config.settings["host"]
        self.MASTER_KEY = config.settings["master_key"]
        self.DATABASE_ID = config.settings["database_id"]
        self.CONTAINER_ID = config.settings[cont]

        self.container = None

    def read_items(self, container):
        print("\nReading all items in a container\n")

        item_list = list(container.read_all_items(max_item_count=10))

        print("Found {0} items".format(item_list.__len__()))

        for doc in item_list:
            print("Item Id: {0}".format(doc.get("id")))

    def scale_container(self, container):
        print("\nScaling Container\n")

        try:
            offer = container.read_offer()
            print(
                "Found Offer and its throughput is '{0}'".format(offer.offer_throughput)
            )

            offer.offer_throughput += 100
            container.replace_throughput(offer.offer_throughput)

            print(
                "Replaced Offer. Offer Throughput is now '{0}'".format(
                    offer.offer_throughput
                )
            )

        except exceptions.CosmosHttpResponseError as e:
            if e.status_code == 400:
                print("Cannot read container throuthput.")
                print(e.http_error_message)
            else:
                raise

    def run_sample(self):
        client = cosmos_client.CosmosClient(
            self.HOST,
            {"masterKey": self.MASTER_KEY},
            user_agent="CosmosDBPythonQuickstart",
            user_agent_overwrite=True,
        )

        try:
            # setup database for this sample
            print("database setup...")
            try:
                db = client.create_database(id=self.DATABASE_ID)
                print("Database with id '{0}' created".format(self.DATABASE_ID))

            except exceptions.CosmosResourceExistsError:
                db = client.get_database_client(self.DATABASE_ID)
                print("Database with id '{0}' was found".format(self.DATABASE_ID))

            # setup container for this sample
            # global container
            print("container setup...")
            try:
                self.container = db.create_container(
                    id=self.CONTAINER_ID,
                    partition_key=PartitionKey(path="/partitionKey"),
                )
                print("Container with id '{0}' created".format(self.CONTAINER_ID))

            except exceptions.CosmosResourceExistsError:
                self.container = db.get_container_client(self.CONTAINER_ID)
                print("Container with id '{0}' was found".format(self.CONTAINER_ID))

            consumer = KafkaConsumer()
            mypartition = TopicPartition("crypto-stream", 0)
            assigned_topic = [mypartition]
            consumer.assign(assigned_topic)

            print("***************")

            for msg in consumer:
                item = json.loads(msg.value)
                print(item)
                # self.container.create_item(body=item)

        except exceptions.CosmosHttpResponseError as e:
            print("\nrun_sample has caught an error. {0}".format(e.message))
        finally:
            print("\nrun_sample done")


if __name__ == "__main__":
    cons = AzureConsumer("container_id")
    cons.run_sample()
