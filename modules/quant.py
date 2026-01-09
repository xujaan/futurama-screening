import numpy as np
import pandas_ta as ta

def calculate_z_score(series, window=20):
    """
    Calculates the Rolling Z-Score.
    Formula: (Value - Mean) / StdDev
    """
    mean = series.rolling(window=window).mean()
    std = series.rolling(window=window).std()
    z_score = (series - mean) / std
    return z_score

def calculate_metrics(df, ticker):
    # 1. Basis
    mark = float(ticker.get('last', 0))
    index = float(ticker.get('info', {}).get('indexPrice', mark))
    basis = (mark - index) / index if index > 0 else 0
    
    # 2. RVOL
    df['Vol_SMA'] = ta.sma(df['volume'], length=20)
    df['RVOL'] = df['volume'] / df['Vol_SMA']
    
    # 3. Z-Score (NEW)
    df['Vol_Z'] = calculate_z_score(df['volume'], window=20)
    z_score = df['Vol_Z'].iloc[-1]
    
    # 4. CVD
    df['delta'] = np.where(df['close'] > df['open'], df['volume'], -df['volume'])
    df['CVD'] = df['delta'].cumsum()
    
    # === SCORING ===
    score = 2 # Base Score
    reasons = []
    
    # RVOL Logic
    rvol = df['RVOL'].iloc[-1]
    if rvol > 5.0:
        score += 1
        reasons.append("Nuclear RVOL")
    elif rvol > 2.0:
        reasons.append("Valid RVOL")
    else:
        reasons.append("Low RVOL")

    # Z-Score Logic (NEW)
    # Z > 3.0 implies statistically significant anomaly (99.7% percentile)
    if z_score > 3.0:
        score += 2
        reasons.append(f"High Z-Score ({z_score:.1f})")
    elif z_score > 1.5:
        score += 1
        reasons.append(f"Mod Z-Score ({z_score:.1f})")

    return df, basis, z_score, score, reasons

def check_fakeout(df, min_rvol):
    if df['RVOL'].iloc[-1] < min_rvol: return False, "Fakeout (Low Vol)"
    return True, ""