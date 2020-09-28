import asyncio
import websockets
import json
import moment
import Bigquery.WrapperBigQuery as WBQ
import CsvWritter.QuoteCsvWriter as csvWriter

msg = \
    {"jsonrpc": "2.0",
     "method": "public/ticker",
     "id": 8106,
     "params": {
         "instrument_name": "BTC-PERPETUAL"}
}

msgforEth =  {"jsonrpc": "2.0",
     "method": "public/ticker",
     "id": 8106,
     "params": {
         "instrument_name": "ETH-PERPETUAL"}
}
msgSuscribeToBTCUSD = \
    {
        "jsonrpc": "2.0",
        "id": 42,
        "method": "public/subscribe",
        "params": {
            "channels": [
                "ticker.ETH_PERPETRUAL.raw",
            ]
        }
    }


msgSuscribeToETHUSD = \
    {
        "jsonrpc": "2.0",
        "id": 42,
        "method": "public/subscribe",
        "params": {
            "channels": [
                "ticker.BTC_PERPETRUAL.raw",
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

getInstruments = {
  "jsonrpc" : "2.0",
  "id" : 7617,
  "method" : "public/get_instruments",
  "params" : {
    "currency" : "BTC",
    "kind" : "index",
    "expired" : False
  }}

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

def prepareJsonForBQ(rawDeribitJson):
    if "params" in rawDeribitJson.keys():
        data = rawDeribitJson["params"]["data"]
        quote = [ {"Plateforme": ["Deribit"], "Asset": [data["index_name"]], "Quote": [data["price"]], "Datetime": [int(data["timestamp"])/1000] }]
        return quote


async def call_api(msgSuscribeToBTCUSD, msgSuscribeToETHUSD):
    async with websockets.connect('wss://test.deribit.com/ws/api/v2') as websocket:
        await websocket.send(msgSuscribeToBTCUSD)
        await websocket.send(msgSuscribeToETHUSD)
        while websocket.open:
            #await websocket.send(suscribeToBTCUSD)
            response = await websocket.recv()
            response_json = json.loads(response)
            print(response_json)
            csv_file = "crypto"+"Deribit"+moment.now().format("DDMMYYYY")+".csv"
            quote = prepareJson(response_json)
            quoteBQ = prepareJsonForBQ(response_json)
            if quoteBQ: WBQ.writeQuotes(quoteBQ, "deribit")
            if quote: csvWriter.writeQuote(csv_file, quote)


asyncio.get_event_loop().run_until_complete(call_api(json.dumps(msgSuscribeToBTCUSD), json.dumps(msgSuscribeToETHUSD)))


