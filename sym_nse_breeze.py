from session_init__.breez_api_session import breeze
import pandas as pd
import numpy as np
import pickle

def store_breeze_syms(nse_symbols) :
    breeze_stock_codes = []
    for nse_sym in nse_symbols :
        try :
            isec_dict = breeze.get_names(exchange_code = 'NSE',stock_code = nse_sym)
            breeze_stock_codes.append(isec_dict['isec_stock_code'])
        except :
            pass    
    return breeze_stock_codes


# NIFTY 50
df= pd.read_csv('ind_nifty50list.csv')
nse_symbols = df['Symbol']
breeze_stock_codes = store_breeze_syms(nse_symbols)
print(breeze_stock_codes)
with open('breeze_nifty50','wb') as file :
    pickle.dump(breeze_stock_codes,file)

# NIFTY 500
df= pd.read_csv('ind_nifty500list.csv')
nse_symbols = df['Symbol']
breeze_stock_codes = store_breeze_syms(nse_symbols)
print(breeze_stock_codes)

with open('breeze_nifty500','wb') as file :
    pickle.dump(breeze_stock_codes,file)


# ENTIRE STOCKS    
file = open('symblist_all.txt')
entire_stcoks = list(file)
entire_stcoks = [ stk.strip('\n') for stk in entire_stcoks ]
breeze_stock_codes = store_breeze_syms(entire_stcoks)
print(breeze_stock_codes,len(breeze_stock_codes))

with open('breeze_all_stocks','wb') as file :
    pickle.dump(breeze_stock_codes,file)  
