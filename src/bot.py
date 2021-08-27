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
from azure_consumer import AzureConsumer
import logging

class TradeBot(AzureConsumer):
    def __init__(self, cont, inventory, vol, funds) -> None:
        self.INVENTORY_ID = config.settings[inventory]
        self.money = funds
        self.volume = vol
        self.inventory = 0
        super().__init__(cont)
        logging.basicConfig(
            filename="./logs/bot.log", level=logging.DEBUG
        )

    def run_sample(self):
        logging.info('Sample run start')
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
                logging.info("Database with id '{0}' created".format(self.DATABASE_ID))

            except exceptions.CosmosResourceExistsError:
                db = client.get_database_client(self.DATABASE_ID)
                logging.warning("Database with id '{0}' was found".format(self.DATABASE_ID))

            # setup container for this sample
            # global container
            print("history container setup...")
            try:
                self.container = db.create_container(
                    id=self.CONTAINER_ID,
                    partition_key=PartitionKey(path="/partitionKey"),
                )
                logging.info("Container with id '{0}' created".format(self.CONTAINER_ID))

            except exceptions.CosmosResourceExistsError:
                self.container = db.get_container_client(self.CONTAINER_ID)
                logging.warning("Container with id '{0}' was found".format(self.CONTAINER_ID))

            print("inventory container setup...")
            try:
                self.container = db.create_container(
                    id=self.INVENTORY_ID,
                    partition_key=PartitionKey(path="/partitionKey"),
                )
                logging.info("Container with id '{0}' created".format(self.INVENTORY_ID))

            except exceptions.CosmosResourceExistsError:
                self.container = db.get_container_client(self.INVENTORY_ID)
                logging.warning("Container with id '{0}' was found".format(self.INVENTORY_ID))

            consumer = KafkaConsumer()
            mypartition = TopicPartition("crypto-stream", 0)
            assigned_topic = [mypartition]
            consumer.assign(assigned_topic)

            print("***************")
            print("I have {} dollars...".format(self.money))
            last_close = None
            last_time = 0
            for msg in consumer:
                item = json.loads(msg.value)
                if (
                    item["periodName"] == "60"
                    and int(item["closetime"]) > last_time + 60
                ):
                    this_close = float(item["ohlc"]["closeStr"])
                    print(
                        "the price is now {} at {}".format(
                            this_close, item["closetime"]
                        )
                    )
                    if last_close == None:
                        self.issue_buy(this_close)
                        print("now i have {} dollars...".format(self.money))
                    elif this_close < last_close:
                        self.issue_buy(this_close)
                        print("now i have {} dollars...".format(self.money))
                    elif this_close > last_close:
                        self.issue_sell(this_close)
                        print("now i have {} dollars...".format(self.money))
                    else:
                        print("i hold...")

                    last_close = this_close
                # self.container.create_item(body=item)

        except exceptions.CosmosHttpResponseError as e:
            print("\nrun_sample has caught an error. {0}".format(e.message))
            logging.error(e.message)

    def issue_buy(self, price):
        if self.money >= (price * self.volume):
            print("i buy {} at {}...".format(self.volume, price))
            self.money -= price * self.volume
            self.inventory += self.volume
        logging.info('Buy success')

    def issue_sell(self, price):
        if self.inventory >= self.volume:
            print("i sell {} at {}...".format(self.volume, price))
            self.money += price * self.volume
            self.inventory -= self.volume
        logging.info('Sell success')


if __name__ == "__main__":
    cons = TradeBot("activity_container", "inventory_container", 0.01, 10000.00)
    cons.run_sample()
