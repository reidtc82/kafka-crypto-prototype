from kafka import KafkaProducer
import time
import cryptowatch as cw
import time
from google.protobuf.json_format import MessageToJson
import json
import uuid

producer = KafkaProducer(bootstrap_servers="localhost:9092", api_version=(0, 10))


def run_sample(subs, sleep_period):
    # Subscribe to resources (https://docs.cryptowat.ch/websocket-api/data-subscriptions#resources)
    cw.stream.subscriptions = subs

    cw.stream.on_trades_update = handle_trades_update
    # Start receiving
    cw.stream.connect()

    time.sleep(sleep_period)

    cw.stream.disconnect()

    # for i in range(100):
    #     time.sleep(sleep_period)


# What to do on each trade update
def handle_trades_update(trade_update):
    """
    trade_update follows Cryptowatch protocol buffer format:
    https://github.com/cryptowatch/proto/blob/master/public/markets/market.proto
    """
    market_msg = ">>> Market#{} Exchange#{} Pair#{}: {} New Trades".format(
        trade_update.marketUpdate.market.marketId,
        trade_update.marketUpdate.market.exchangeId,
        trade_update.marketUpdate.market.currencyPairId,
        len(trade_update.marketUpdate.tradesUpdate.trades),
    )
    print(market_msg)

    for trade in trade_update.marketUpdate.tradesUpdate.trades:
        trade_msg = "\tID:{} TIMESTAMP:{} TIMESTAMPNANO:{} PRICE:{} AMOUNT:{}".format(
            trade.externalId,
            trade.timestamp,
            trade.timestampNano,
            trade.priceStr,
            trade.amountStr,
        )
        print(trade_msg)

        item = json.loads(MessageToJson(trade))
        item["id"] = str(uuid.uuid4())
        msg = json.dumps(item)
        producer.send("crypto-stream", bytes(msg, encoding="utf8"))


if __name__ == "__main__":
    run_sample(["markets:*:trades"], 1)
