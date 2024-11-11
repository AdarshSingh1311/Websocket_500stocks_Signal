import numpy as np
import pandas as pd
import pickle
import funcs


# DUMMY DATA
"""
with open('prev_data.pickle','rb') as file :
        dct = pickle.load(file)

O,H,L,C,V,STKS = dct.values()

prev_day_v = V[1]
prev_day_c = C[1]   """


def alok_sir_alpha(perc_v,perc_c,bb_lb,bb_mult,sup_lb,sup_mult,
                   O,H,L,C,V,STKS,prev_day_v,prev_day_c) : 

    #global STKS
    #global O,H,L,C,V
    #global prev_day_v,prev_day_c
    list_to_df = lambda x : pd.DataFrame(x,columns=STKS.values).ffill().bfill()

    pd.set_option("future.no_silent_downcasting", True)
    
    O,H,L,C,V = list(map(list_to_df,[O,H,L,C,V]))

    
    # FILTER    
    total_vol = V.cumsum(axis = 0).iloc[-1].values
    volume_filter = total_vol > perc_v*prev_day_v
    price = C.iloc[-1].values
    price_filter = price > perc_c*prev_day_c

    filter_ = volume_filter & price_filter
    filter_ = filter_ + 0

    # BB SIGNAL
    change = C.pct_change(fill_method=None)*C.shift(1)
    std = change.rolling(bb_lb).std()
    mean = C.rolling(bb_lb).mean()
    bb_ub = mean + bb_mult*std
    bb_lb = mean - bb_mult*std

    bb_signal = (C > bb_ub) & (C.shift(1) < bb_ub.shift(1))
    bb_signal = bb_signal + 0
    bb_signal = bb_signal.iloc[-1].values

    # SUP SIGNAL
    true_range = H - L
    atr = true_range.rolling(sup_lb).mean()

    high_lb = H.rolling(sup_lb).apply(lambda x : max(x))
    low_lb = L.rolling(sup_lb).apply(lambda x : min(x))
    mean_lb = (high_lb + low_lb)/2


    st_ub = mean_lb + sup_mult*atr
    st_lb = mean_lb - sup_mult*atr

    sup_signal = (C >= st_ub) | (C<=st_lb)
    sup_signal = sup_signal.ffill().replace(True,1).replace(False,-1)
    sup_signal = sup_signal.iloc[-1].values
    
    # ALPHA
    alpha_signals = filter_*bb_signal*sup_signal
    dct_ = { stk : signal for stk,signal in zip(STKS.stock.values,alpha_signals)  }
    print(dct_)
    

    return alpha_signals



#print( alok_sir_alpha(1,1,20,2,7,2)  )





  

