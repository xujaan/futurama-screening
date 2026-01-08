import numpy as np
from scipy.signal import argrelextrema
from modules.config_loader import CONFIG

def check_alignment(values, tolerance=0.015):
    if len(values) < 2: return False
    avg = np.mean(values)
    return all(abs(v - avg) / avg < tolerance for v in values)

def find_pattern(df):
    if len(df) < 50: return None
    df_idx = df.reset_index(drop=True)
    n = 3
    df_idx['min_local'] = df_idx.iloc[argrelextrema(df_idx.low.values, np.less_equal, order=n)[0]]['low']
    df_idx['max_local'] = df_idx.iloc[argrelextrema(df_idx.high.values, np.greater_equal, order=n)[0]]['high']
    peaks = df_idx[df_idx['max_local'].notnull()]['max_local'].values
    valleys = df_idx[df_idx['min_local'].notnull()]['min_local'].values
    
    enabled = CONFIG['patterns']
    
    if enabled.get('double_bottom') and check_alignment(valleys[-2:], 0.01): return 'double_bottom'
    if enabled.get('double_top') and check_alignment(peaks[-2:], 0.01): return 'double_top'
    if enabled.get('bullish_rectangle') and len(peaks) > 3: return 'bullish_rectangle'
    
    return None