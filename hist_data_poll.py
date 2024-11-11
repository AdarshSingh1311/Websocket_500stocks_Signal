from import_all_modules import *
from session_init__.breez_api_session import breeze
import funcs
import threading

def get_stock_eq_data(stock,from_,now_,granularity,eq_db_bool = False) :
    global eq_db
    def clean_parse_data (data_df) :
        col = ['open','high', 'low', 'close','volume']
        data_df = data_df.set_index('datetime',drop = True)
        data_df[col] = data_df[col].astype(float)
        return data_df[col]
    
    try :
        json = breeze.get_historical_data_v2(interval=granularity,
                                        from_date=  from_,
                                        to_date= now_,
                                        stock_code= stock,
                                        exchange_code="NSE",
                                        product_type="cash")

        data_df= pd.DataFrame(json['Success'])

        if data_df.empty : return
        print(stock)

    except Exception as e :
        print(e)
        print(json)
        return

    if not eq_db_bool  : return data_df
    
    eq_db[stock] = clean_parse_data(data_df)


def eq_database(insts,max_api_limit_per_min = 100) :
    global eq_db,from_,now_,granularity
    slice = int((len(insts)/max_api_limit_per_min) + 1)
    chunks = int(len(insts)/slice)
    indxes = np.arange(0,len(insts),chunks)[:slice]
    
    chunks_insts = []
    for indx in range(len(indxes)) :
        if indx < len(indxes)-1 : chunks_insts.append(insts[indxes[indx]:indxes[indx+1]]) 
        else : chunks_insts.append(insts[indxes[indx]:])    

    for chunk_insts in chunks_insts :

        print(chunks_insts)
        insts = chunk_insts
        threads = [threading.Thread(target=get_stock_eq_data,args=(inst,from_,now_,granularity,True)) for inst in insts ]
        for thread in threads : thread.start()
        for thread in threads : thread.join()
        
        if chunks_insts[-1] == chunk_insts : return eq_db
        print('100 API CALLS IN A MINUTE LIMIT REACHED')
        print('---WAITING 1 MINUTE TO PASS --')
        time.sleep(60)
         
    return eq_db    

def stich_insts (col,custom_func = None,custom_indx = None) :
    global eq_db
    scores = []
    for inst in eq_db.keys() :
        print(inst)
        if custom_func is None : 
            if col == '%' : ser = eq_db[inst]['close'].pct_change().bfill()*100
            else : ser = eq_db[inst][col] 
        else : 
            df = eq_db[inst]
            #else : df = eq_db[inst].loc[custom_indx]
            ser = custom_func(df)

        scores.append(ser) 

    #print(scores)
    scores_df = pd.concat(scores,axis = 1) 
    scores_df.columns = eq_db.keys() 
    #scores_df.index =  [ pd.to_datetime(indx).date() for indx in scores_df.index ]
    scores_df.sort_index(inplace= True)

    if custom_indx is not None : return scores_df.loc[custom_indx]
    return scores_df 

if __name__ == '__main__' :
    trail_dt = lambda x : x + "T07:00:00.000Z" if isinstance(x,str) else str(x) + "T07:00:00.000Z" 
    now_ = datetime.datetime.now().date()
    from_ = now_ - timedelta(days = 3)
    from_,now_ = trail_dt(from_),trail_dt(now_)
    granularity = '1minute'
    eq_db = {}
    stocks = funcs.get_breeze_stk_code_nif500()
    eq_db = eq_database(stocks)
    stiched_df =  stich_insts('close')

    o =  stich_insts('open').values.tolist()
    h = stich_insts('high').values.tolist()
    l = stich_insts('low').values.tolist()
    c = stiched_df.values.tolist()
    v = stich_insts('volume').values.tolist()

    print( stiched_df )

    prev_data = {'o':o,'h':h,'l':l,'c':c,'v':v ,'stk_names' : stiched_df.columns }

    with open('prev_data.pickle','wb') as file :
        pickle.dump(prev_data,file)
        

      