import socketio

LIVE_OHLC_STREAM_URL = "https://breezeapi.icicidirect.com"
LIVE_STREAM_URL = "https://livestream.icicidirect.com"
LIVE_FEEDS_URL = "https://livefeeds.icicidirect.com"

sio = socketio.Client()

sio.connect(LIVE_OHLC_STREAM_URL)

@sio.event
def connect() :
    print(' CONNECTED TO BREEZE WS SERVER !!')

@sio.event
def response(data) :
    print(f'received data is {data} ' )   

try :
    sio.wait()
    
except :
    print('disconnecting to  breeze ws server !!!')
    sio.disconnect()
