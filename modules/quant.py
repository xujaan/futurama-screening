import numpy as np
import pandas_ta as ta

def calculate_metrics(df, ticker):
    # Basis
    mark = float(ticker.get('last', 0))
    index = float(ticker.get('info', {}).get('indexPrice', mark))
    basis = (mark - index) / index if index > 0 else 0
    
    # RVOL
    df['Vol_SMA'] = ta.sma(df['volume'], length=20)
    df['RVOL'] = df['volume'] / df['Vol_SMA']
    
    # CVD
    df['delta'] = np.where(df['close'] > df['open'], df['volume'], -df['volume'])
    df['CVD'] = df['delta'].cumsum()
    
    # Score
    score = 2
    reasons = []
    rvol = df['RVOL'].iloc[-1]
    
    if rvol > 5.0: score += 1; reasons.append(f"Nuclear RVOL")
    elif rvol > 2.0: reasons.append(f"Valid RVOL")
    else: reasons.append(f"Low RVOL")

    return df, basis, score, reasons

def check_fakeout(df, min_rvol):
    current_rvol = df['RVOL'].iloc[-1]
    if current_rvol < min_rvol:
        return False, f"Fakeout (RVOL {current_rvol:.1f})"
    return True, ""