import json
import websockets
import asyncio
import moment
import CsvWritter.QuoteCsvWriter as csvWriter
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
	#"event": "ping",
	"pair": ["XBT/USD", "XBT/EUR", "ETH/USD", "ETH/EUR"],
	#"subscription": {"name": "ticker"}
	#"subscription": {"name": "spread"}
	"subscription": {"name": "trade"}
	#"subscription": {"name": "book", "depth": 10}
	#"subscription": {"name": "ohlc", "interval": 5}
}

def cleanAssetName(rawAssetName):
    rawAssetNameClean = rawAssetName.replace("/", "_")
    rawAssetNameClean = rawAssetNameClean.replace("XBT", "btc")
    return rawAssetNameClean.lower()

def prepareJson(rawDeribitJson):
    if  type(rawDeribitJson) == list and len(rawDeribitJson) > 1:
        data = rawDeribitJson[1]
        asset = cleanAssetName(rawDeribitJson[3])
        quote = [ {"Plateforme": "Kraken", "Asset": asset, "Quote": data[0][0], "Datetime": data[0][2] }]
        return quote


async def call_api(msg):
    async with websockets.connect('wss://ws.kraken.com') as websocket:
        print(msg)
        await websocket.send(msg)
        while websocket.open:
            #await websocket.send(suscribeToBTCUSD)
            response = await websocket.recv()
            response_json = json.loads(response)
            csv_file = "crypto" + "Kraken" + moment.now().format("DDMMYYYY")+".csv"
            quote = prepareJson(response_json)
            if quote: csvWriter.writeQuote(csv_file, quote)

asyncio.get_event_loop().run_until_complete(call_api(json.dumps(msg)))