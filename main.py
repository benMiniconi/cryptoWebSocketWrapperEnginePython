import PlateformeWrapper.DeribitWrapper as deribit
import PlateformeWrapper.coinbasePro as coinbase
import PlateformeWrapper.krakenWrapper as kraken
import asyncio
import json

from flask import Flask

app = Flask(__name__)

loop = asyncio.get_event_loop()


@app.route('/')
def hello_worls():
    return "Hello Guys! "


@app.route('/deribitstatus')
def deribitWSStatus():
    return deribit.websocketStatus()


@app.route('/krakenstatus')
def krakenWSStatus():
    return kraken.websocketStatus()


@app.route('/coinbasestatus')
def coinbaseWSStatus():
    return coinbase.websocketStatus()


@app.route('/deribitrunsocket')
def deribitWSRun():
    loop = asyncio.new_event_loop()
    import threading
    t = threading.Thread(target=deribit.runWebSocket, args=(loop,))
    t.start()
    return "Ok"


@app.route('/coinbaserunsocket')
def coinbaseWSRun():
    loop = asyncio.new_event_loop()
    import threading
    t = threading.Thread(target=coinbase.runWebSocket, args=(loop,))
    t.start()
    return "Ok"


@app.route('/krakenrunsocket')
def krakenWSRun():
    loop = asyncio.new_event_loop()
    import threading
    t = threading.Thread(target=kraken.runWebSocket, args=(loop,))
    t.start()
    return "Ok"
