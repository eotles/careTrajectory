'''
Created on Jun 13, 2016

@author: eotles
'''
import multiprocessing as mp
import pandas as pd

#globals
_pt_col='ptID'
_enc_col='encID'
_enc_date_col='date'
_index_col='index'


def set_globals(pt_col, enc_col, enc_date_col, index_col):
    global _pt_col
    global _enc_col
    global _enc_date_col
    global _index_col
    _pt_col = pt_col
    _enc_col = enc_col
    _enc_date_col = enc_date_col
    _index_col = index_col

def prep_indices(encs, indices):
    #if indices not provided then make all encounters an index
    if not indices:
        indices = encs[[_enc_col]]
        indices[_index_col] = True
    
    #make df of patient indices
    _indices = pd.merge(indices, encs[[_pt_col, _enc_col]], 
                          how='left', on=_enc_col)[[_pt_col, _enc_col]]
    
    return(_indices)

def analyze_pt(pt_encs, pt_index_enc):
    tmp_pt_df = pt_encs
    tmp_pt_df['index'] = pt_index_enc
    #offset
    enc_ids = pt_encs[_enc_col].tolist()
    num_enc = len(enc_ids)
    index_enc_pos = enc_ids.index(pt_index_enc)
    offset = [i-index_enc_pos for i in xrange(num_enc)]
    tmp_pt_df['offset'] = offset
    #date dif
    index_enc_dt = pt_encs[pt_encs[_enc_col]==pt_index_enc].iloc[0][_enc_date_col]
    tmp_pt_df['dif'] = tmp_pt_df[_enc_date_col] - index_enc_dt
    print(tmp_pt_df)
    return(tmp_pt_df)

def analyze_pt_pl(argDict):
    pt_encs = argDict['pt_encs']
    pt_index_enc = argDict['pt_index_enc']
    tmp_pt_df = pt_encs
    tmp_pt_df['index'] = pt_index_enc
    #offset
    enc_ids = pt_encs[_enc_col].tolist()
    num_enc = len(enc_ids)
    index_enc_pos = enc_ids.index(pt_index_enc)
    offset = [i-index_enc_pos for i in xrange(num_enc)]
    tmp_pt_df['offset'] = offset
    #date dif
    index_enc_dt = pt_encs[pt_encs[_enc_col]==pt_index_enc].iloc[0][_enc_date_col]
    tmp_pt_df['dif'] = tmp_pt_df[_enc_date_col] - index_enc_dt
    return(tmp_pt_df)

def analyze_pl(argDict):
    pt_id = argDict['pt_id']
    pt_index_enc = argDict['pt_index_enc']
    pt_encs = argDict['pt_encs']
    
    tmp_pt_df = pt_encs
    tmp_pt_df['index'] = pt_index_enc
    #offset
    enc_ids = pt_encs[_enc_col].tolist()
    num_enc = len(enc_ids)
    index_enc_pos = enc_ids.index(pt_index_enc)
    offset = [i-index_enc_pos for i in xrange(num_enc)]
    tmp_pt_df['offset'] = offset
    #date dif
    index_enc_dt = pt_encs[pt_encs[_enc_col]==pt_index_enc].iloc[0][_enc_date_col]
    tmp_pt_df['dif'] = tmp_pt_df[_enc_date_col] - index_enc_dt
    return(tmp_pt_df)
    

def main(encs, pt_col='ptID', enc_col='encID', enc_date_col='date', index_col='index', indices=False):
    set_globals(pt_col, enc_col, enc_date_col, index_col)
    
    #TODO: check that provided dfs have columns                     
    _indices = prep_indices(encs, indices)
    
    #make list of patients to check
    pt_ids = _indices[_pt_col].unique()
    
    for pt_id in pt_ids:
        pt_encs = encs[encs[_pt_col]==pt_id]
        pt_encs.sort_values(by=_enc_date_col)
        
        pt_indices = _indices[_indices[_pt_col]==pt_id][_enc_col].unique()
        for pt_index_enc in pt_indices:
            analyze_pt(pt_encs, pt_index_enc)


def main_pt_pl(encs, pt_col='ptID', enc_col='encID', enc_date_col='date', index_col='index', indices=False):
    set_globals(pt_col, enc_col, enc_date_col, index_col)
    
    #TODO: check that provided dfs have columns                  
    _indices = prep_indices(encs, indices)
    
    #make list of patients to check
    pt_ids = _indices[_pt_col].unique()
    
    for pt_id in pt_ids:
        pt_encs = encs[encs[_pt_col]==pt_id]
        pt_encs.sort_values(by=_enc_date_col)
        
        pt_indices = _indices[_indices[_pt_col]==pt_id][_enc_col].unique()
        args = []
        for pt_index_enc in pt_indices:
            argDict = {}
            #argDict['pt_encs'] = pt_encs.copy(deep=True)
            argDict['pt_encs'] = pt_encs
            argDict['pt_index_enc'] = pt_index_enc
            args.append(argDict)
        pool = mp.Pool()
        results = pool.map(analyze_pt_pl, args)
        pool.close()
        pool.join()
        print(results)


def main_pl(encs, pt_col='ptID', enc_col='encID', enc_date_col='date', index_col='index', indices=False):
    set_globals(pt_col, enc_col, enc_date_col, index_col)
    
    #TODO: check that provided dfs have columns                  
    _indices = prep_indices(encs, indices)
    
    #make list of patients to check
    pt_ids = _indices[_pt_col].unique()
    
    args = []
    for pt_id in pt_ids:
        pt_indices = _indices[_indices[_pt_col]==pt_id][_enc_col].unique()
        pt_encs = encs[encs[_pt_col]==pt_id]
        for pt_index_enc in pt_indices:
            argDict = {}
            argDict['pt_id'] = pt_id
            argDict['pt_index_enc'] = pt_index_enc
            argDict['pt_encs'] = pt_encs
            args.append(argDict)
    
    pool = mp.Pool()
    results = pool.map(analyze_pl, args)
    pool.close()
    pool.join()
    results = pd.concat(results)
    print(results.head())

if __name__ == '__main__':
    main()