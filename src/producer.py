from kafka import KafkaProducer
import time
import cryptowatch as cw
from google.protobuf.json_format import MessageToJson
import json
import uuid
import logging
import os
import sys

logging.basicConfig(
    filename="./logs/producer.log", level=logging.DEBUG
)

try:
    producer = KafkaProducer(bootstrap_servers="localhost:9092", api_version=(0, 10))
    logging.info("Kafka producer creatded")
except:
    logging.error("Kafka producer not created")


def run_trade_updates(subs, sleep_period):
    # Subscribe to resources (https://docs.cryptowat.ch/websocket-api/data-subscriptions#resources)
    cw.stream.subscriptions = subs

    cw.stream.on_trades_update = handle_trades_update
    # Start receiving
    try:
        cw.stream.connect()
        logging.info("Trade update stream connected")
    except:
        logging.error("Error connecting to stream trade update")

    time.sleep(sleep_period)

    try:
        cw.stream.disconnect()
        logging.info("Trade update stream disconnected")
    except:
        logging.error("Trade update stream could not disconnect")


def run_ohlc_updates(subs, sleep_period):
    # Subscribe to resources (https://docs.cryptowat.ch/websocket-api/data-subscriptions#resources)
    cw.stream.subscriptions = subs

    cw.stream.on_intervals_update = handle_intervals_update
    # Start receiving

    try:
        cw.stream.connect()
        logging.info("OHLC stream connected")
    except:
        logging.error("OHLC stream could not connect")

    time.sleep(sleep_period)

    try:
        cw.stream.disconnect()
        logging.info("OHLC stream disconnected")
    except:
        logging.error("OHLC stream could not disconnect")


# What to do on each trade update
def handle_trades_update(trade_update):
    market_msg = ">>> Market#{} Exchange#{} Pair#{}: {} New Trades".format(
        trade_update.marketUpdate.market.marketId,
        trade_update.marketUpdate.market.exchangeId,
        trade_update.marketUpdate.market.currencyPairId,
        len(trade_update.marketUpdate.tradesUpdate.trades),
    )
    print(market_msg)
    logging.info(market_msg)

    for trade in trade_update.marketUpdate.tradesUpdate.trades:
        trade_msg = "\tID:{} TIMESTAMP:{} TIMESTAMPNANO:{} PRICE:{} AMOUNT:{}".format(
            trade.externalId,
            trade.timestamp,
            trade.timestampNano,
            trade.priceStr,
            trade.amountStr,
        )
        print(trade_msg)
        logging.info(trade_msg)

        item = json.loads(MessageToJson(trade))
        item["id"] = str(uuid.uuid4())
        msg = json.dumps(item)
        try:
            producer.send("crypto-stream", bytes(msg, encoding="utf8"))
        except:
            logging.error("Error sending trade message")


def handle_intervals_update(interval_update):
    market_msg = ">>> Market#{} Exchange#{} Pair#{}".format(
        interval_update.marketUpdate.market.marketId,
        interval_update.marketUpdate.market.exchangeId,
        interval_update.marketUpdate.market.currencyPairId,
    )
    print(market_msg)
    logging.info(market_msg)

    temp = []
    for interval in interval_update.marketUpdate.intervalsUpdate.intervals:
        interval_msg = (
            "\tTIMESTAMP:{} OHLC:{}, {}, {}, {} VOLUMEBASE: {} VOLUMEQUOTE: {}".format(
                interval.closetime,
                interval.ohlc.openStr,
                interval.ohlc.highStr,
                interval.ohlc.lowStr,
                interval.ohlc.closeStr,
                interval.volumeBaseStr,
                interval.volumeQuoteStr,
            )
        )
        print(interval_msg)

        item = json.loads(MessageToJson(interval))
        item["id"] = str(uuid.uuid4())
        msg = json.dumps(item)
        try:
            producer.send("crypto-stream", bytes(msg, encoding="utf8"))
        except:
            logging.error("Error sending OHLC message")


if __name__ == "__main__":
    # run_trade_updates(["markets:*:trades"], 1)
    logging.info("Starting producer")
    run_ohlc_updates(["assets:60:ohlc"], 1)
