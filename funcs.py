import pickle 

def get_breeze_stk_code_nif50 () :
    with open('breeze_nifty50','rb') as file :
        return pickle.load(file)
    
def get_breeze_stk_code_nif500 () :
    with open('breeze_nifty500','rb') as file :
        return pickle.load(file)  

def get_breeze_stk_code_entire_stocks ()  :
    with open('breeze_all_stocks','rb') as file :
        return pickle.load(file)  

def split_list_to_chunks(list_,chunks) :
    list_chunks = []
    chunk_len = int(len(list_)/chunks)

    for chunk_no in range(chunks) :
        strat_indx = chunk_no*chunk_len
        list_chunks.append(list_[strat_indx : strat_indx + chunk_len])

    return list_chunks   
    
    