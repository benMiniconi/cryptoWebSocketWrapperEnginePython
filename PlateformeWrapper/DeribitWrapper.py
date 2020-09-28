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
msgSuscribeToBTCUSD = {"jsonrpc": "2.0",
     "method": "public/subscribe",
     "id": 42,
     "params": {
         "channels": ["ticker.BTC-PERPETUAL.raw"]}
     }


msgSuscribeToETHUSD = {"jsonrpc": "2.0",
     "method": "public/subscribe",
     "id": 42,
     "params": {
         "channels": ["ticker.ETH-PERPETUAL.raw"]}
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
buffer = []


def cleanAssetName(rawAssetName):
    rawAssetNameClean = rawAssetName.replace("-", "_")
    rawAssetNameClean = rawAssetNameClean.replace("PERPETUAL", "USD")
    return rawAssetNameClean.lower()

def prepareJson(rawDeribitJson):
    if "params" in rawDeribitJson.keys():
        data = rawDeribitJson["params"]["data"]
        cleanName = cleanAssetName(data["instrument_name"])
        quote = {"Plateforme": "Deribit", "Asset": cleanName, "Quote": data["index_price"], "Datetime": int(data["timestamp"])/1000, "Bid": data["best_bid_price"], "BidAmount": data["best_bid_amount"], "Ask":  data["best_ask_price"], "AskAmount": data["best_ask_amount"], "OpenInterest": data["open_interest"]}
        return quote


def manageBuffer(quote):
    if quote:
        buffer.append(quote)
        if len(buffer) >= 1000:
            WBQ.writeQuotes(buffer, "deribit")
            emptyBuffer()
            return True
        else:
            return False


def emptyBuffer():
    global buffer
    buffer = []


async def call_api(msgSuscribeToBTCUSD, msgSuscribeToETHUSD):
    async with websockets.connect('wss://www.deribit.com/ws/api/v2', ping_interval=None) as websocket:
        await websocket.send(msgSuscribeToBTCUSD)
        await websocket.send(msgSuscribeToETHUSD)
        while websocket.open:
            #await websocket.send(suscribeToBTCUSD)
            response = await websocket.recv()
            response_json = json.loads(response)
            #print(response_json)
            csv_file = "crypto"+"Deribit"+moment.now().format("DDMMYYYY")+".csv"
            quote = prepareJson(response_json)
            if quote: csvWriter.writeQuote(csv_file, [quote])
            manageBuffer(quote)



asyncio.get_event_loop().run_until_complete(call_api(json.dumps(msgSuscribeToBTCUSD), json.dumps(msgSuscribeToETHUSD)))


