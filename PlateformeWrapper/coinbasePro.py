# # btc_heartbeat.py
import asyncio
import websockets
import json
import moment
import datetime
import CsvWritter.QuoteCsvWriter as csvWriter
from Bigquery import WrapperBigQuery as WBQ
#
# from copra.websocket import Channel, Client
#
# loop = asyncio.get_event_loop()
#
# ws = Client(loop, Channel('heartbeat', 'BTC-USD'))
#
# try:
#     loop.run_forever()
# except KeyboardInterrupt:
#     loop.run_until_complete(ws.close())
#     loop.close()

websocketCoin = ""
bufferCoinbase = []



msg = {
    "type": "subscribe",
    "product_ids": [
        "ETH-USD",
        "ETH-EUR",
        "BTC-USD",
        "BTC-EUR"
    ],
    "channels": ["ticker"]
}

def manageBuffer(quote):
    if quote:
        bufferCoinbase.append(quote)
        if len(bufferCoinbase) >= 1000:
            WBQ.writeQuotes(bufferCoinbase, "coinbase")
            emptyBuffer()
            return True
        else:
            return False


def emptyBuffer():
    global bufferCoinbase
    bufferCoinbase = []


def cleanAssetName(rawAssetName):
    rawAssetNameClean = rawAssetName.replace("-", "_")
    rawAssetNameClean = rawAssetNameClean.replace("XBT", "btc")
    return rawAssetNameClean.lower()

def prepareJson(rawDeribitJson):
    if type(rawDeribitJson) == dict and "price" in rawDeribitJson.keys():
        data = rawDeribitJson
        asset = cleanAssetName(data['product_id'])
        timesta = datetime.datetime.now().timestamp()
        quote = {"Plateforme": "Coinbase", "Asset": asset, "Quote": data["price"], "Datetime": timesta , "Bid": float(data["best_bid"]), "BidAmount": 0, "Ask":  float(data["best_ask"]), "AskAmount": 0, "OpenInterest": float(data["open_24h"])}
        return quote


async def call_api(msg):
    async with websockets.connect('wss://ws-feed.pro.coinbase.com') as websocketCoin:
        print(msg)
        await websocketCoin.send(msg)

        while websocketCoin.open:
            #await websocket.send(suscribeToBTCUSD)
            response = await websocketCoin.recv()
            response_json = json.loads(response)
            #response_json
            csv_file = "crypto" + "Coinbase" + moment.now().format("DDMMYYYY")+".csv"
            quote = prepareJson(response_json)
            if quote: csvWriter.writeQuote(csv_file, [quote])
            manageBuffer(quote)


asyncio.get_event_loop().run_until_complete(call_api(json.dumps(msg)))