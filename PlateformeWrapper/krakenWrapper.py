import json
import websockets
import asyncio
import moment
import datetime
import CsvWritter.QuoteCsvWriter as csvWriter
from Bigquery import WrapperBigQuery as WBQ

websocketKraken = ""
websocketKraken2 = ""
bufferKraken = []

# for i in range(3):
# 	try:
# 		ws = create_connection("wss://ws.kraken.com")
# 	except Exception as error:
# 		print('Caught this error: ' + repr(error))
# 		time.sleep(3)
# 	else:
# 		break

msg = {
    "event": "subscribe",
    # "event": "ping",
    "pair": ["XBT/USD", "XBT/EUR", "ETH/USD", "ETH/EUR"],
    "subscription": {"name": "ticker"}
    # "subscription": {"name": "spread"}
    # "subscription": {"name": "trade"}
    # "subscription": {"name": "book", "depth": 10}
    # "subscription": {"name": "ohlc", "interval": 5}
}


def cleanAssetName(rawAssetName):
    rawAssetNameClean = rawAssetName.replace("/", "_")
    rawAssetNameClean = rawAssetNameClean.replace("XBT", "btc")
    return rawAssetNameClean.lower()


def prepareJson(rawDeribitJson):
    if type(rawDeribitJson) == list and len(rawDeribitJson) > 1:
        data = rawDeribitJson[1]
        asset = cleanAssetName(rawDeribitJson[3])
        quote = {"Plateforme": "Kraken", "Asset": asset, "Quote": float(data["c"][0]),
                 "Datetime": datetime.datetime.now().timestamp(), "Bid": float(data["b"][0]),
                 "BidAmount": float(data["b"][2]),
                 "Ask": float(data["a"][0]), "AskAmount": float(data["a"][2]), "OpenInterest": float(data["p"][0])}
        return quote


def manageBuffer(quote):
    if quote:
        bufferKraken.append(quote)
        if len(bufferKraken) >= 300:
            WBQ.writeQuotes(bufferKraken, "kraken")
            emptyBuffer()
            return True
        else:
            return False


def emptyBuffer():
    global bufferKraken
    bufferKraken = []


async def manageAnswer(Wsocket, plateforme):
    response = await Wsocket.recv()
    response_json = json.loads(response)
    # response_json
    csv_file = "crypto" + plateforme + moment.now().format("DDMMYYYY") + ".csv"
    quote = prepareJson(response_json)
    if quote: csvWriter.writeQuote(csv_file, [quote])
    manageBuffer(quote)


async def reconnect(msg):
    async with websockets.connect('wss://ws.kraken.com', ping_interval=None) as websocketKraken2:
        print(msg)
        await websocketKraken2.send(msg)
        while websocketKraken2.open:
            # await websocket.send(suscribeToBTCUSD)
            try:
                await manageAnswer(websocketKraken2, "Coinbase")
            except websockets.exceptions.ConnectionClosedOK:
                print("ConnectionClosedOK")
                await asyncio.sleep(60)
                await call_api(msg)
            except websockets.exceptions.ConnectionClosedError:
                print("ConnectionClosedError")
                await asyncio.sleep(60)
                await call_api(msg)
            except websockets.exceptions.ConnectionClosed:
                print("ConnectionClosedOK")
                await asyncio.sleep(60)
                await call_api(msg)


async def call_api(msg):
    async with websockets.connect('wss://ws.kraken.com', ping_interval=None) as websocketKraken:
        print(msg)
        await websocketKraken.send(msg)
        while websocketKraken.open:
            # await websocket.send(suscribeToBTCUSD)
            try:
                await manageAnswer(websocketKraken, "Coinbase", )
            except websockets.exceptions.ConnectionClosedOK:
                print("ConnectionClosedOK")
                await asyncio.sleep(60)
                await reconnect(msg)
            except websockets.exceptions.ConnectionClosedError:
                print("ConnectionClosedError")
                await asyncio.sleep(60)
                await reconnect(msg)
            except websockets.exceptions.ConnectionClosed:
                print("ConnectionClosedOK")
                await asyncio.sleep(60)
                await reconnect(msg)


asyncio.get_event_loop().run_until_complete(call_api(json.dumps(msg)))
