import os
from dotenv import load_dotenv

from breeze_connect import BreezeConnect

import funcs

import multiprocessing
import threading
from multiprocessing import Manager,Lock

from import_all_modules import*
import asyncio
#import logging
#logging.basicConfig(level= 'DEBUG')

from alpha import alok_sir_alpha

session_token_ = 48831334
load_dotenv()
api_key_ = os.getenv('api_key')
api_secret_ = os.getenv('api_secret')


def vertical_to_horizontal_ticks(vertical_ticks,stock_order) :
    new_cols = vertical_ticks.columns.values.tolist()[1:]
    vertical_ticks = vertical_ticks[new_cols]
    vertical_ticks = vertical_ticks.set_index('stock',drop = True)
    vertical_ticks = vertical_ticks.astype(float)

    vertical_ticks = stock_order.join(vertical_ticks,on='stock')
    
    #Horizontal Ticks
    o = vertical_ticks.o.values.tolist()
    h = vertical_ticks.h.values.tolist()
    l = vertical_ticks.l.values.tolist()
    c = vertical_ticks.c.values.tolist()
    v = vertical_ticks.v.values.tolist()

    return o,h,l,c,v

def multi_process_args(processes) :
    breeze_instances = None
    #breeze_instances = create_multiple_breeze_instances(processes)
    stocks = funcs.get_breeze_stk_code_nif500()
    stock_chunks = funcs.split_list_to_chunks(stocks,processes)
    stock_order = pd.DataFrame(stocks,columns = ['stock'])
    
    """
    collect= []
    for stk_chunk in stock_chunks :
        collect += stk_chunk
    
    print(len(stocks),len(collect))
    assert len(stocks) == len(collect) """
    #stock_chunks = [['TCS']]  

    print( f'breeze instances : {breeze_instances} ' )
    print( f'stock chunks  : {stock_chunks} ' )

    return breeze_instances,stock_chunks,stock_order


def live_ws_data(stocks_list,db_list) :
    
    global BreezeConnect
    global api_key_,api_secret_,session_token_

    breeze = BreezeConnect(api_key=api_key_)
    breeze.generate_session(api_secret=api_secret_,
                                session_token=session_token_)
    
    breeze.ws_connect()

    def on_ticks(ticks):
        stk_name = ticks.get('stock_code')
        o,h,l,c,v = ticks.get('open'),ticks.get('high'),ticks.get('low'),ticks.get('close'),ticks.get('volume')
        timestamp = ticks.get('datetime')

        db_list.append([timestamp,stk_name,o,h,l,c,v])
        print([timestamp,stk_name,o,h,l,c,v])

    breeze.on_ticks = on_ticks

    for stk in stocks_list :
        breeze.subscribe_feeds(exchange_code="NSE", stock_code=stk, product_type="", expiry_date="",
                            strike_price="", right="", interval="1minute")
            
        
    while True :
        pass    


if __name__ == '__main__' :
    
    processes = 6
    
    breeze_instances,stock_chunks,stock_order= multi_process_args(processes)

    db_queue = multiprocessing.Queue() 

    cols = ['timestamp','stock','o','h','l','c','v']
    ts_now = lambda  : datetime.datetime.now()
    stop_func  = lambda x : ts_now() > x
    dt_str_breeze = lambda x : x.strftime('%Y-%m-%d %H:%M')

    min_to_sec = lambda x  : x + ':00'
    minute_delay = lambda x,y : x - timedelta(minutes=y)
    
    # HIST DATA
    with open('prev_data.pickle','rb') as file :
        dct = pickle.load(file)
    
    O,H,L,C,V,STKS = dct.values()
    STKS = pd.DataFrame(STKS,columns=['stock'])
    prev_day_v = np.array(V).cumsum()[-1]
    prev_day_c = np.array(C[-1])

    with Manager() as manager :

        db_list = manager.list()

        multi_processes = [ multiprocessing.Process(target = live_ws_data, args = (stocks_list,db_list,)) 
                                                                for stocks_list in stock_chunks ] 

        for process in multi_processes : process.start() 

        strat_time =ts_now()
        stop_time = strat_time + timedelta(minutes= 10)
        
        prev_ts = dt_str_breeze(ts_now())
        while not stop_func(stop_time) :

            # CLOCK BEAT
            curr_ts = ts_now()
            prev_candle_ts = dt_str_breeze(minute_delay(curr_ts,2))
            candle_ts = dt_str_breeze(minute_delay(curr_ts,1))
            curr_ts = dt_str_breeze(curr_ts)

            if curr_ts != prev_ts :

                df = pd.DataFrame(list(db_list),columns=cols)
                #print(df)
                try :
                    vertical_ticks = df[df.timestamp == min_to_sec(prev_candle_ts) ]
                    #print(vertical_ticks)
                    o,h,l,c,v = vertical_to_horizontal_ticks(vertical_ticks,STKS)
                    print(f' O : {o} \n\n  H : {h} \n\n  L : {l} \n\n  C : {c} \n\n  V : {v} ')
                    
                    O.append(o)
                    H.append(h)
                    L.append(l)
                    C.append(c)
                    V.append(v)

                    signals = alok_sir_alpha(1, 1, 20, 2, 7, 2,
                                             O, H, L, C, V, STKS,
                                             prev_day_v,prev_day_c)

                    O = O[1:]
                    H = H[1:]
                    L = L[1:]
                    C = C[1:]
                    V = V[1:]

                except Exception as e :
                    print(f'EXCEPTION : {e}')

                prev_ts = curr_ts

        
        for process in multi_processes : process.kill()
        for process in multi_processes : process.join() 
        
        #results = [] 
        #while not db_queue.empty() :
        #    results.append(db_queue.get())

        results = db_list

        print(f'collected db : {results}')
        
        # 1 sec consistency check
        results = list(results)
        db_df = pd.DataFrame(results,columns=['timestamp','stock','o','h','l','c','v'])
        consistency_df= db_df.groupby('timestamp').apply(lambda x : len(x),include_groups=False)
        print(f'Small TF : {consistency_df} ' )

        # 3 sec consistency check
        higher_tf_ts = consistency_df.asfreq('300s').index.values
        higher_tf_ts_df = pd.DataFrame(higher_tf_ts,index=higher_tf_ts,columns=['higher_tf'])
        
        db_df.timestamp = pd.to_datetime(db_df.timestamp)
        db_df = db_df.join(higher_tf_ts_df,on='timestamp')
        db_df.sort_values(by='timestamp',inplace=True)
        db_df.higher_tf = db_df.higher_tf.bfill()
        consistency_df_htf = db_df.groupby('higher_tf').apply(lambda x : len(x.stock.unique()),include_groups=False)
        print(f'Large TF {consistency_df_htf} ' ) 
        

        with open('db','wb') as file :
            pickle.dump(results,file)
