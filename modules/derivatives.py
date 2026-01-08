import numpy as np
from scipy.stats import linregress

def get_slope(series):
    try: return linregress(np.arange(len(series)), np.array(series))[0]
    except: return 0

def analyze_derivatives(df, ticker, side):
    score = 1
    reasons = []
    
    # 1. Funding
    funding = float(ticker.get('info', {}).get('fundingRate', 0))
    if side == "Long" and funding > 0.02: 
        return False, 0, ["Funding Overheated"]
    
    if abs(funding) < 0.01:
        score += 1
        reasons.append(f"Cool Funding")

    # 2. Basis
    mark = float(ticker.get('last', 0))
    index = float(ticker.get('info', {}).get('indexPrice', mark))
    basis = (mark - index) / index if index > 0 else 0
    
    # 3. CVD Divergence
    price_slope = get_slope(df['close'].iloc[-10:])
    cvd_slope = get_slope(df['CVD'].iloc[-10:])
    
    if price_slope > 0 and cvd_slope < 0:
        if side == "Short": score += 2; reasons.append("Bear CVD Div (+2)")
        elif side == "Long": score -= 2; reasons.append("Against Bear CVD (-2)")
    elif price_slope < 0 and cvd_slope > 0:
        if side == "Long": score += 2; reasons.append("Bull CVD Div (+2)")
        elif side == "Short": score -= 2; reasons.append("Against Bull CVD (-2)")

    return True, score, reasons