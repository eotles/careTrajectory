'''
Created on Jun 24, 2016

@author: eotles
'''

import pandas as pd
import multiprocessing as mp

#globals
_pt_col='ptID'
_enc_col='encID'
_date_col='date'

def set_globals(pt_col, enc_col, enc_date_col):
    global _pt_col
    global _enc_col
    global _date_col
    _pt_col = pt_col
    _enc_col = enc_col
    _date_col = enc_date_col

def calc_pt_traj_extract(argDict):
    data = argDict['data']
    pt_id = argDict['pt_id']
    
    pt_data = data[data[_pt_col] == pt_id]
    pt_data['offset'] = pt_data[_date_col] - pt_data[_date_col].iloc[0]
    pt_traj = pt_data.set_index(_enc_col)['offset'].to_dict()
    
    results = (pt_id, pt_traj)
    return(results)
 
#time = 0.0062*(num_pts) + 0.5249
def traj_extract_pl(data, lim=None):    
    pt_ids = data[_pt_col].unique()
    if lim is not None: pt_ids = pt_ids[0:lim]
    args = [{'data': data, 'pt_id': pt_id} for pt_id in pt_ids]
    
    pool = mp.Pool()
    results = pool.map(calc_pt_traj_extract, args)
    pool.close()
    pool.join()
    
    results = dict(results)
    return(results)
 
def calc_enc_traj(pt_lookup, pt_trajs, index_enc_id):
    pt_id = pt_lookup[index_enc_id]
    pt_traj = pt_trajs[pt_id]
    index_offset = pt_traj[index_enc_id]
    enc_traj = {key:[] for key in [_enc_col, _pt_col, 'offset', 'index']}
    for enc_id, offset in pt_traj.iteritems():
        reindxd_offset = (offset - index_offset)
        reindxd_offset_days = reindxd_offset.days
        enc_traj[_enc_col].append(enc_id)
        enc_traj[_pt_col].append(pt_id)
        enc_traj['offset'].append(reindxd_offset_days)
        enc_traj['index'].append(index_enc_id)
    return(enc_traj)

def make_enc_traj_df(data, pt_col='MRN', enc_col='CSN', date_col='date', lim=None):
    set_globals(pt_col, enc_col, date_col)
    pt_trajs = traj_extract_pl(data, lim=lim)
    pt_lookup = data[data[pt_col].isin(pt_trajs.keys())]
    pt_lookup = pt_lookup.set_index(enc_col)[pt_col].to_dict()
    
    enc_trajs = []
    for index_enc_id in pt_lookup.keys():
        enc_trajs.append(calc_enc_traj(pt_lookup, pt_trajs, index_enc_id))
    
    all_enc_trajs = dict()
    for key in [_enc_col, _pt_col, 'offset', 'index']:
        all_enc_trajs[key] = [item for enc_pile in enc_trajs for item in enc_pile[key]]
    
    enc_traj_df = pd.DataFrame(all_enc_trajs)
    return(enc_traj_df)

def count_pile(pile_df, window=365):
    for col in [_enc_col, _pt_col]:
        #print(col)
        tmp_pile_df = pile_df.groupby('offset')[col].nunique()
        tmp_pile_df = tmp_pile_df.ix[-window:window]
        pd.set_option('display.max_rows', len(tmp_pile_df))
        #print('%s' %(tmp_pile_df.sort_index().to_string(dtype=False)))
        pd.reset_option('display.max_rows')
        return(tmp_pile_df)
    
def analyze(enc_traj_df, index_flags, window=365):
    def cnt_fx_le(cnt):
        return(cnt.ix[-window:-1].cumsum())
    
    def cnt_fx_ge(cnt):
        return(cnt.ix[1:window].sum() - cnt.ix[1:window-1].cumsum())
    
    data = {}

    enc_traj_df = enc_traj_df[(enc_traj_df['offset']>= -window) &
                              (enc_traj_df['offset']<= window) ]

    m_df = index_flags.merge(enc_traj_df, on='index', how='left')
    for flag, flag_fx, cnt_fx in [('<=', lambda x:x<=0, cnt_fx_le), 
                                  ('>=', lambda x:x>=0, cnt_fx_ge)]:

        flag_pile = m_df[(m_df['flag'] == flag) &
                         (m_df['offset'].apply(flag_fx))]
        cnt = count_pile(flag_pile, window=window)
        data[flag] = {'cnt': cnt_fx(cnt), '0': cnt.ix[0:0]}
        
    denom = data['<=']['0'].ix[0]
    le_series = data['<=']['cnt'].append(data['<=']['0'])
    ge_series = data['>=']['0'].append(data['>=']['cnt'])
    tmp_df = pd.concat([le_series,ge_series], axis=1)
    tmp_df.columns = ['LE', 'GE']
    res_data = {'df': tmp_df, 'denom':denom}
    return(res_data)

if __name__ == '__main__':
    pass