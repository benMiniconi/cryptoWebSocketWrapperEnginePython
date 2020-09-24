import asyncio
import websockets
import json
import moment
import CsvWritter.QuoteCsvWriter as csvWriter

msg = \
    {"jsonrpc": "2.0",
     "method": "public/get_index_price",
     "id": 42,
     "params": {
         "index_name": "btc_usd"}
     }

msgSuscribeToBTCUSD = \
    {
        "jsonrpc": "2.0",
        "id": 3600,
        "method": "public/subscribe",
        "params": {
            "channels": [
                "deribit_price_index.btc_usd", "deribit_price_index.eth_usd",
            ]
        }
    }

hearbeat = \
    {
        "jsonrpc": "2.0",
        "id": 9098,
        "method": "public/set_heartbeat",
        "params": {
            "interval": 10
        }
    }

websocket = ""

async def setHeartbeat():
    asyncio.get_event_loop().run_until_complete(call_api(json.dumps(hearbeat)))
    return "OK"


async def startTunnel():
    asyncio.get_event_loop().run_until_complete(call_api(json.dumps(msg)))
    return "OK"


async def subscribeToBTCUSDCall():
    return "OK"




def prepareJson(rawDeribitJson):
    if "params" in rawDeribitJson.keys():
        data = rawDeribitJson["params"]["data"]
        quote = [ {"Plateforme": "Deribit", "Asset": data["index_name"], "Quote": data["price"], "Datetime": int(data["timestamp"])/1000 }]
        return quote


async def call_api(msg):
    async with websockets.connect('wss://test.deribit.com/ws/api/v2') as websocket:
        print(msg)
        await websocket.send(msg)

        while websocket.open:
            #await websocket.send(suscribeToBTCUSD)
            response = await websocket.recv()
            response_json = json.loads(response)
            #response_json
            csv_file = "crypto"+"Deribit"+moment.now().format("DDMMYYYY")+".csv"
            quote = prepareJson(response_json)
            if quote: csvWriter.writeQuote(csv_file, quote)


asyncio.get_event_loop().run_until_complete(call_api(json.dumps(msgSuscribeToBTCUSD)))


