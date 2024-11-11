from session_init__.breez_api_session import breeze
import funcs

import asyncio
import time
import datetime
from datetime import timedelta
import pickle

stocks = funcs.get_breeze_stk_code_nif50()
print(stocks)

def sheduler (stk_name,c) :
    pass


breeze.ws_connect()

passed_timestamps = []
timestamp_pointer = None
stocks_close_dict = {}

#list_ = []

def on_ticks(ticks):
    #print("Ticks: {}".format(ticks))
    global timestamp_pointer,stocks_close_dict,passed_timestamps

    stk_name = ticks.get('stock_code')
    o,h,l,c,v = ticks.get('open'),ticks.get('high'),ticks.get('low'),ticks.get('close'),ticks.get('volume')
    timestamp = ticks.get('datetime')

    #list_.append([timestamp,stk_name,o,h,l,c,v])

    #print( f'{timestamp} : {stk_name} :  ' ,{'o': o , 'h': h, 'l': l, 'c': c ,'v':v} )

    
    if timestamp != timestamp_pointer and timestamp not in passed_timestamps :
        timestamp_pointer = timestamp
        print( f'{timestamp} :: {stocks_close_dict}  \n')
        print(f'stocks count : {len(stocks_close_dict)}')
        passed_timestamps.append(timestamp)
        stocks_close_dict = {}   


    else :
        stocks_close_dict[stk_name] = c   


breeze.on_ticks = on_ticks

#stocks = ['TCS', 'WIPRO', 'ITC', 'TATMOT','RELIND']
for stk in stocks :

    breeze.subscribe_feeds(exchange_code="NSE", stock_code=stk, product_type="", expiry_date="",
                           strike_price="", right="", interval="1second")

strat_time = datetime.datetime.now()
stop_func  = lambda x : datetime.datetime.now() >= x
stop_time = strat_time + timedelta(minutes= 3)

while not stop_func(stop_time) :
    pass
 

#with open('db','wb') as file :
#    pickle.dump(list_,file)
    



