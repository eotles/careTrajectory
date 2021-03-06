import pandas as pd
import multiprocessing as mp
import matplotlib
import matplotlib.pyplot as plt

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
    _pt_col = argDict['_pt_col']
    _enc_col = argDict['_enc_col']
    _date_col = argDict['_date_col'] 
    
    pt_data = data[data[_pt_col] == pt_id]
    pt_data['offset'] = pt_data[_date_col] - pt_data[_date_col].iloc[0]
    pt_traj = pt_data.set_index(_enc_col)['offset'].to_dict()
    
    results = (pt_id, pt_traj)
    return(results)
 
def traj_extract_pl(data, lim=None, processes=mp.cpu_count()-1):    
    pt_ids = data[_pt_col].unique()
    if lim is not None: pt_ids = pt_ids[0:lim]
    args = [{'data': data, 'pt_id': pt_id, 
             '_pt_col': _pt_col,
             '_enc_col': _enc_col,
             '_date_col': _date_col} for pt_id in pt_ids]
    
    pool = mp.Pool(processes=processes)
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

def main(data, pt_col='MRN', enc_col='CSN', date_col='date', lim=None):
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
    extract = traj_extract(enc_traj_df, pt_col, enc_col, date_col)
    return(extract)

class traj_extract():
    def __init__(self, df, pt_col='MRN', enc_col='CSN', date_col='date'):
        self.df = df
        self.pt_col = pt_col
        self.enc_col = enc_col
        self.date_col = date_col
        
        self.all_comer_curve = self._curve_counter(list(df['index'].unique()))
    
    def _curve_counter(self, idxs):
        traj = self.df[self.df['index'].isin(idxs)]
        window = self.df['offset'].abs().max()
        
        pts_on_day = {}
        for i in xrange(-window,window+1):
            pts_on_day[i] = list(traj[traj['offset']==i][self.pt_col].unique())
        
        pts_middle_out = {i: pts_on_day[i] for i in [-1, 0, 1]}
        for offset in xrange(2, window+1):
            for sense in [-1,1]:
                curr = offset*sense
                prev = curr-sense
                pts_middle_out[curr] = pts_middle_out[prev] + pts_on_day[curr]
        pts_middle_out = {k: set(v) for k,v in pts_middle_out.iteritems()}
        
        pts_cnt_middle_out = {i: len(pt_list) for i,pt_list in pts_middle_out.iteritems()}
        curve = {i: pts_cnt_middle_out[0]-v for i,v in pts_cnt_middle_out.iteritems()}
        curve[0] = pts_cnt_middle_out[0]
        
        return(curve)
    
    def plot_fig(self, idxs, name='curve', window=None,  all_comer=True, gauges=[]):
        def prep_curve(curve_counter, name):
            curve = pd.Series(curve_counter)
            curve = curve/curve.max()
            curve.name = name
            return(curve)
        
        idxs_curve = self._curve_counter(idxs)
        idxs_curve = prep_curve(idxs_curve, name=name)
        curves = pd.DataFrame(idxs_curve)
        if all_comer:
            ac_name='*All Comers*'
            ac_curve = prep_curve(self.all_comer_curve, name=ac_name)
            curves[ac_name] = ac_curve
            
        if window is not None:
            curves = curves.loc[-window:window]

        plt.figure(1)
        plt.figure(figsize = (9,4))
        gs1 = matplotlib.gridspec.GridSpec(4, 9)
        gs1.update(wspace=0, hspace=0.05)
        ax1 = plt.subplot(gs1[:,0:8])
        ax2 = plt.subplot(gs1[:,8], sharey=ax1)
        curves.plot(ax=ax1, kind='line', ylim=[0,1])
        color='k'
        for gauge in gauges:
            ax2.axhline(y=gauge, color=color)
            color='r'
        plt.setp(ax2.get_xaxis(), visible=False)
        plt.show()
        
        
        '''
        plt.figure(1)
        plt.figure(figsize = (9,4))
        gs1 = matplotlib.gridspec.GridSpec(8, 17)
        gs1.update(wspace=0, hspace=0.05)
        
        ax1 = plt.subplot(gs1[:,0:8])
        ax2 = plt.subplot(gs1[:,8], sharey=ax1)
        ax3 = plt.subplot(gs1[:,9:17], sharey=ax1)
        
        curves.loc[:0].plot(ax=ax1, kind='line', ylim=[0,1], legend=False)
        for gauge in gauges:
            ax2.axhline(y=gauge)
        curves.loc[0:].plot(ax=ax3, kind='line', ylim=[0,1])
        
        plt.setp(ax2.get_xaxis(), visible=False)
        
        plt.show()
        '''
        return(curves)


if __name__ == '__main__':
    main()
