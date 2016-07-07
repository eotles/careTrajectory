# -*- coding: utf-8 -*-
"""
Created on Thu Mar 03 16:45:22 2016
 
@author: eotles
"""
import datetime
import pandas as pd
 
def load_data(dbFp):
    import emod_util as eu
    i2_s1 = eu.extract(dbFp, 'i2_s1')
    i2_s2 = eu.extract(dbFp, 'i2_s2')
    i2_s2 = i2_s2[['CSN', 'TS Arrive']]
    i2_s2['TS Arrive'] = i2_s2['TS Arrive'].astype('datetime64[ns]')
    Dx = eu.extract(dbFp, 'Dx')
    data = i2_s1.merge(i2_s2, how='left', on='CSN')
    data = data.merge(Dx, how='left', on='CSN')
    data = data.sort(columns='TS Arrive', ascending=True)
    return(data)    
   
def _get_col_val_targets(data, columns, substrings, verbose=True):
    target_dict = {}
    for col in columns:
        if(verbose):print(col)
        col_val_counts = data[col].value_counts()
        val_list = []
        for col_val in col_val_counts.keys():
            if(type(col_val) is not str): col_val = str(col_val)
            for substring in substrings:
                if(substring.lower() in col_val.lower()): 
                    if(verbose): 
                        print('\t%s: %d' %(col_val, col_val_counts[col_val]))
                    val_list.append(col_val)
        target_dict[col] = val_list
    return(target_dict)
 
def _make_is_index(data, target_dict):
    col_data = []
    for col, values in target_dict.iteritems():
        col_data.append(data[col].isin(values))
    col_data = pd.DataFrame(col_data).transpose()
    is_index = col_data.apply(lambda x: any(x), axis=1)
    return(is_index)
 
#TODO: make this also have an exact match
def make_is_index(data, columns, substrings, exact=False, verbose=True):
    if(exact):
        target_dict = {col: substrings for col in substrings}
    else:
        target_dict = _get_col_val_targets(data, columns, substrings, 
                                       verbose=verbose)
    is_index = _make_is_index(data, target_dict)
    if(verbose): print('Total index records: %d' %(is_index.sum()))
    return(_make_is_index(data, target_dict))   
 
#NOTE: This window is < not <= !
def get_vs(data, is_index, window = 180):
    window = datetime.timedelta(days=window)
    index_df = data[is_index]
    tmp_df_list = []
    for ISN, row in enumerate(zip(index_df['MRN'], index_df['TS Arrive'], 
                                  index_df['CSN'])):
        #if(ISN > 50): break
        pt_id = row[0]
        idx_enc_dt = row[1]
        idx_enc_csn = row[2]
        dr_end = idx_enc_dt + window
        dr_start = idx_enc_dt - window
        tmp_df = data[data['MRN'] == pt_id]
        tmp_df = tmp_df[tmp_df['TS Arrive'] > dr_start]
        num_prev = tmp_df[tmp_df['TS Arrive'] < idx_enc_dt].shape[0]
        tmp_df = tmp_df[tmp_df['TS Arrive'] < dr_end]
        num_totl = tmp_df.shape[0]
        from_idx = [i for i in xrange(-num_prev, num_totl-num_prev)]
        tmp_df['Index_SN'] = ISN
        tmp_df['Index_CSN'] = idx_enc_csn
        tmp_df['From_Index'] = from_idx
        tmp_df_list.append(tmp_df)
    return(pd.concat(tmp_df_list))
 
def merge_index_info(data, vs):
    index_data = data
    drop_cols  = [col for col in data.columns if 'Index' in col]
    index_data = index_data.drop(drop_cols, axis=1)
    index_cols = {col: 'Index_%s' %(col) for col in index_data.columns}
    index_data = index_data.rename(columns=index_cols)
    return(vs.merge(index_data, on='Index_CSN', how='left'))
 
#make_is_index doesn't do exact matches
#TODO: Make the CSN and MRN columns flexible
def get_vs_wIndex_info(data, columns, substrings, window=180, exact=False, verbose=True):
    is_index = make_is_index(data, columns, substrings, exact=exact, verbose=verbose)
    vs = get_vs(data, is_index, window=window)
    vs = merge_index_info(data, vs)
    return(vs)