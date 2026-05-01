"""
High win-rate manual scalp signal model.

This module builds Telegram-style setups: entry zone, tight partial targets,
and breakeven after TP2. It is analysis-only; execution stays manual unless
another caller explicitly trades the generated signal.
"""

import math
from typing import Dict, List, Optional

import pandas as pd

from modules.technicals import calculate_atr, detect_regime


DEFAULT_CONFIG = {
    "enabled": False,
    "timeframes": ["15m"],
    "allow_longs": True,
    "allow_shorts": False,
    "min_rvol": 1.35,
    "min_adx": 18.0,
    "min_natr": 0.25,
    "max_natr": 7.5,
    "entry_atr_near": 0.25,
    "entry_atr_far": 0.95,
    "sl_atr": 0.85,
    "swing_lookback": 18,
    "tp_atr_multipliers": [0.80, 1.20, 1.70, 2.30, 3.00, 4.00],
    "tp_splits": [0.70, 0.15, 0.08, 0.04, 0.02, 0.01],
    "move_sl_to_be_after_tp": 1,
    "max_entry_distance_pct": 0.45,
    "min_score": 8,
    "max_sl_pct": 1.35,
    "min_tp2_r": 0.85,
    "min_runner_r": 2.00,
    "cool_funding_abs": 0.0008,
    "require_sma200_alignment": True,
    "min_trend_spread_atr": 0.08,
    "min_ema_slope_atr": 0.015,
    "max_extension_atr": 1.35,
    "pullback_lookback": 5,
    "max_last_range_atr": 2.20,
    "max_last_body_atr": 1.10,
    "max_opposite_wick_ratio": 0.45,
    "min_close_position_long": 0.52,
    "max_close_position_short": 0.48,
    "require_momentum_turn": True,
}


def get_high_wr_config(config: Optional[Dict] = None) -> Dict:
    cfg = DEFAULT_CONFIG.copy()
    if config:
        cfg.update(config)
    return cfg


def is_enabled_for_timeframe(timeframe: str, config: Optional[Dict] = None) -> bool:
    cfg = get_high_wr_config(config)
    return bool(cfg.get("enabled")) and str(timeframe) in set(cfg.get("timeframes", []))


def _safe_float(value, default=0.0) -> float:
    try:
        if value is None:
            return default
        value = float(value)
        if math.isnan(value) or math.isinf(value):
            return default
        return value
    except Exception:
        return default


def _last(df: pd.DataFrame, column: str, default=0.0) -> float:
    if column not in df.columns or df.empty:
        return default
    return _safe_float(df[column].iloc[-1], default)


def _rvol(df: pd.DataFrame, window=20) -> float:
    if len(df) < window + 1:
        return 0.0
    if "RVOL" in df.columns:
        return _last(df, "RVOL")
    vol_sma = df["volume"].rolling(window).mean()
    df["RVOL"] = df["volume"] / vol_sma
    avg = _safe_float(vol_sma.iloc[-1])
    if avg <= 0:
        return 0.0
    return _safe_float(df["volume"].iloc[-1]) / avg


def _funding_rate(ticker: Optional[Dict]) -> float:
    if not ticker:
        return 0.0
    info = ticker.get("info", {}) if isinstance(ticker, dict) else {}
    return _safe_float(info.get("fundingRate"), 0.0)


def _candle_stats(row) -> Dict[str, float]:
    high = _safe_float(row.get("high"))
    low = _safe_float(row.get("low"))
    open_ = _safe_float(row.get("open"))
    close = _safe_float(row.get("close"))
    candle_range = max(high - low, 1e-12)
    body = abs(close - open_)
    upper_wick = high - max(open_, close)
    lower_wick = min(open_, close) - low
    close_pos = (close - low) / candle_range
    return {
        "range": candle_range,
        "body": body,
        "upper_wick": max(upper_wick, 0.0),
        "lower_wick": max(lower_wick, 0.0),
        "close_pos": close_pos,
    }


def _ema_slope_atr(df: pd.DataFrame, column: str, atr: float, lookback: int = 5) -> float:
    if atr <= 0 or column not in df.columns or len(df) <= lookback:
        return 0.0
    current = _safe_float(df[column].iloc[-1])
    previous = _safe_float(df[column].iloc[-1 - lookback])
    return (current - previous) / atr


def _recent_pullback_touch(df: pd.DataFrame, side: str, lookback: int) -> bool:
    if len(df) < 2:
        return False
    recent = df.iloc[-max(2, lookback):]
    ema_fast = recent["EMA_Fast"] if "EMA_Fast" in recent.columns else None
    ema_slow = recent["EMA_Slow"] if "EMA_Slow" in recent.columns else None
    if ema_fast is None or ema_slow is None:
        return False
    if side == "Long":
        touched_fast = (recent["low"] <= ema_fast).any()
        touched_slow = (recent["low"] <= ema_slow).any()
        reclaimed = _safe_float(df["close"].iloc[-1]) >= min(_last(df, "EMA_Fast"), _last(df, "EMA_Slow"))
    else:
        touched_fast = (recent["high"] >= ema_fast).any()
        touched_slow = (recent["high"] >= ema_slow).any()
        reclaimed = _safe_float(df["close"].iloc[-1]) <= max(_last(df, "EMA_Fast"), _last(df, "EMA_Slow"))
    return bool((touched_fast or touched_slow) and reclaimed)


def _quality_gate(df: pd.DataFrame, side: str, atr: float, cfg: Dict) -> (bool, List[str]):
    reasons = []
    if atr <= 0 or len(df) < 8:
        return False, ["invalid ATR"]

    close = _last(df, "close")
    ema_fast = _last(df, "EMA_Fast")
    ema_slow = _last(df, "EMA_Slow")
    sma50 = _last(df, "SMA_50", ema_slow)
    sma200 = _last(df, "SMA_200", sma50)
    macd_h = _last(df, "MACD_h")
    macd_prev = _safe_float(df["MACD_h"].iloc[-2]) if "MACD_h" in df.columns and len(df) > 1 else macd_h
    stoch_k = _last(df, "stoch_rsi_k", 50)
    stoch_prev = _safe_float(df["stoch_rsi_k"].iloc[-2], stoch_k) if "stoch_rsi_k" in df.columns and len(df) > 1 else stoch_k

    trend_spread = abs(ema_fast - ema_slow) / atr
    min_trend_spread = float(cfg.get("min_trend_spread_atr", 0.0))
    if trend_spread < min_trend_spread:
        return False, [f"weak EMA spread {trend_spread:.2f} ATR"]

    slope = _ema_slope_atr(df, "EMA_Slow", atr)
    min_slope = float(cfg.get("min_ema_slope_atr", 0.0))
    if side == "Long":
        if not (ema_fast > ema_slow and close > ema_slow and close >= sma50):
            return False, ["trend alignment failed"]
        if cfg.get("require_sma200_alignment", True) and sma200 > 0 and close < sma200:
            return False, ["below SMA200"]
        if slope < min_slope:
            return False, [f"EMA slope too flat {slope:.3f} ATR"]
    else:
        if not (ema_fast < ema_slow and close < ema_slow and close <= sma50):
            return False, ["trend alignment failed"]
        if cfg.get("require_sma200_alignment", True) and sma200 > 0 and close > sma200:
            return False, ["above SMA200"]
        if slope > -min_slope:
            return False, [f"EMA slope too flat {slope:.3f} ATR"]
    reasons.append("trend aligned")

    extension = abs(close - ema_slow) / atr
    if extension > float(cfg.get("max_extension_atr", 99.0)):
        return False, [f"overextended {extension:.2f} ATR"]

    if not _recent_pullback_touch(df, side, int(cfg.get("pullback_lookback", 5))):
        return False, ["no recent EMA pullback reclaim"]
    reasons.append("EMA pullback reclaim")

    stats = _candle_stats(df.iloc[-1])
    if stats["range"] / atr > float(cfg.get("max_last_range_atr", 99.0)):
        return False, [f"last candle range too large {stats['range'] / atr:.2f} ATR"]
    if stats["body"] / atr > float(cfg.get("max_last_body_atr", 99.0)):
        return False, [f"last candle body too large {stats['body'] / atr:.2f} ATR"]

    if side == "Long":
        if stats["upper_wick"] / stats["range"] > float(cfg.get("max_opposite_wick_ratio", 1.0)):
            return False, ["upper wick rejection"]
        if stats["close_pos"] < float(cfg.get("min_close_position_long", 0.0)):
            return False, ["weak candle close"]
        if cfg.get("require_momentum_turn", True) and not (macd_h >= macd_prev and stoch_k >= stoch_prev):
            return False, ["momentum not turning up"]
    else:
        if stats["lower_wick"] / stats["range"] > float(cfg.get("max_opposite_wick_ratio", 1.0)):
            return False, ["lower wick rejection"]
        if stats["close_pos"] > float(cfg.get("max_close_position_short", 1.0)):
            return False, ["weak candle close"]
        if cfg.get("require_momentum_turn", True) and not (macd_h <= macd_prev and stoch_k <= stoch_prev):
            return False, ["momentum not turning down"]
    reasons.append("clean trigger candle")

    return True, reasons


def _score_long(df: pd.DataFrame, rvol: float, adx: float, funding: float, cfg: Dict) -> (int, List[str]):
    score, reasons = 0, []
    close = _last(df, "close")
    ema_fast = _last(df, "EMA_Fast")
    ema_slow = _last(df, "EMA_Slow")
    sma50 = _last(df, "SMA_50", ema_slow)
    stoch_k = _last(df, "stoch_rsi_k", 50)
    macd_h = _last(df, "MACD_h")

    prev = df.iloc[-2]
    last = df.iloc[-1]

    if ema_fast > ema_slow and close >= ema_slow:
        score += 2
        reasons.append("EMA trend up")
    if close >= sma50:
        score += 1
        reasons.append("above SMA50")
    if _safe_float(prev["low"]) <= ema_slow <= close or _safe_float(last["low"]) <= ema_fast <= close:
        score += 2
        reasons.append("pullback reclaim")
    if 35 <= stoch_k <= 82:
        score += 1
        reasons.append("momentum not overheated")
    if macd_h > 0:
        score += 1
        reasons.append("MACD positive")
    if len(df) > 1 and "MACD_h" in df.columns and macd_h >= _safe_float(df["MACD_h"].iloc[-2]):
        score += 1
        reasons.append("MACD improving")
    if len(df) > 1 and "stoch_rsi_k" in df.columns and stoch_k >= _safe_float(df["stoch_rsi_k"].iloc[-2], stoch_k):
        score += 1
        reasons.append("stoch turning up")
    if rvol >= cfg["min_rvol"]:
        score += 1
        reasons.append(f"RVOL {rvol:.2f}x")
    if adx >= cfg["min_adx"]:
        score += 1
        reasons.append(f"ADX {adx:.1f}")
    if abs(funding) <= cfg["cool_funding_abs"]:
        score += 1
        reasons.append("cool funding")
    return score, reasons


def _score_short(df: pd.DataFrame, rvol: float, adx: float, funding: float, cfg: Dict) -> (int, List[str]):
    score, reasons = 0, []
    close = _last(df, "close")
    ema_fast = _last(df, "EMA_Fast")
    ema_slow = _last(df, "EMA_Slow")
    sma50 = _last(df, "SMA_50", ema_slow)
    stoch_k = _last(df, "stoch_rsi_k", 50)
    macd_h = _last(df, "MACD_h")

    prev = df.iloc[-2]
    last = df.iloc[-1]

    if ema_fast < ema_slow and close <= ema_slow:
        score += 2
        reasons.append("EMA trend down")
    if close <= sma50:
        score += 1
        reasons.append("below SMA50")
    if _safe_float(prev["high"]) >= ema_slow >= close or _safe_float(last["high"]) >= ema_fast >= close:
        score += 2
        reasons.append("pullback reject")
    if 18 <= stoch_k <= 65:
        score += 1
        reasons.append("momentum not exhausted")
    if macd_h < 0:
        score += 1
        reasons.append("MACD negative")
    if len(df) > 1 and "MACD_h" in df.columns and macd_h <= _safe_float(df["MACD_h"].iloc[-2]):
        score += 1
        reasons.append("MACD weakening")
    if len(df) > 1 and "stoch_rsi_k" in df.columns and stoch_k <= _safe_float(df["stoch_rsi_k"].iloc[-2], stoch_k):
        score += 1
        reasons.append("stoch turning down")
    if rvol >= cfg["min_rvol"]:
        score += 1
        reasons.append(f"RVOL {rvol:.2f}x")
    if adx >= cfg["min_adx"]:
        score += 1
        reasons.append(f"ADX {adx:.1f}")
    if abs(funding) <= cfg["cool_funding_abs"]:
        score += 1
        reasons.append("cool funding")
    return score, reasons


def _targets(entry: float, atr: float, side: str, cfg: Dict) -> List[float]:
    mults = cfg.get("tp_atr_multipliers") or DEFAULT_CONFIG["tp_atr_multipliers"]
    if side == "Long":
        return [entry + atr * float(m) for m in mults]
    return [entry - atr * float(m) for m in mults]


def _build_signal(
    symbol: str,
    timeframe: str,
    df: pd.DataFrame,
    ticker: Optional[Dict],
    side: str,
    score: int,
    reasons: List[str],
    cfg: Dict,
) -> Optional[Dict]:
    close = _last(df, "close")
    atr = _last(df, "ATR_14")
    if atr <= 0:
        atr = _safe_float(calculate_atr(df, length=14))
    if close <= 0 or atr <= 0:
        return None

    quality_ok, quality_reasons = _quality_gate(df, side, atr, cfg)
    if not quality_ok:
        return None
    reasons.extend(quality_reasons)

    natr = (atr / close) * 100
    if natr < cfg["min_natr"] or natr > cfg["max_natr"]:
        return None

    lookback = int(cfg.get("swing_lookback", 18))
    recent = df.iloc[-lookback:]
    near = float(cfg["entry_atr_near"]) * atr
    far = float(cfg["entry_atr_far"]) * atr
    sl_atr = float(cfg["sl_atr"]) * atr

    if side == "Long":
        entry_high = close - near
        entry_low = max(float(recent["low"].min()), close - far)
        if entry_low >= entry_high:
            entry_low = entry_high - (atr * 0.35)
        entry = (entry_low + entry_high) / 2
        swing_sl = float(recent["low"].min()) - atr * 0.20
        sl = max(swing_sl, entry_low - sl_atr)
        if sl >= entry:
            return None
    else:
        entry_low = close + near
        entry_high = min(float(recent["high"].max()), close + far)
        if entry_low >= entry_high:
            entry_high = entry_low + (atr * 0.35)
        entry = (entry_low + entry_high) / 2
        swing_sl = float(recent["high"].max()) + atr * 0.20
        sl = min(swing_sl, entry_high + sl_atr)
        if sl <= entry:
            return None

    entry_dist = abs(close - entry) / entry * 100
    if entry_dist > float(cfg["max_entry_distance_pct"]):
        return None

    tps = _targets(entry, atr, side, cfg)
    risk = abs(entry - sl)
    if risk <= 0:
        return None

    risk_pct = risk / entry * 100
    if risk_pct > float(cfg.get("max_sl_pct", 1.35)):
        return None

    tp2_reward = abs(tps[1] - entry)
    if tp2_reward / risk < float(cfg.get("min_tp2_r", 0.85)):
        return None

    runner_r = abs(tps[-1] - entry) / risk
    if runner_r < float(cfg.get("min_runner_r", 2.0)):
        return None

    splits = cfg.get("tp_splits") or DEFAULT_CONFIG["tp_splits"]
    tp_plan = [
        {"price": float(price), "close_ratio": float(splits[i]) if i < len(splits) else 0.0}
        for i, price in enumerate(tps)
    ]

    total_score = int(score)
    return {
        "Symbol": symbol,
        "Side": side,
        "Timeframe": timeframe,
        "Pattern": "HIGH_WR_SCALP",
        "Mode": "HIGH_WR_SCALP",
        "Entry": float(entry),
        "Entry_Low": float(min(entry_low, entry_high)),
        "Entry_High": float(max(entry_low, entry_high)),
        "SL": float(sl),
        "TP1": float(tps[0]),
        "TP2": float(tps[1]),
        "TP3": float(tps[2]),
        "TP_Plan": tp_plan,
        "RR": round(runner_r, 2),
        "Tech_Score": total_score,
        "Quant_Score": 0,
        "Deriv_Score": 0,
        "SMC_Score": 0,
        "Basis": 0.0,
        "Z_Score": 0.0,
        "Zeta_Score": 50.0,
        "OBI": 0.0,
        "NATR": float(natr),
        "BTC_Bias": "",
        "Reason": "High WR trend pullback",
        "Tech_Reasons": ", ".join(reasons),
        "Quant_Reasons": f"NATR {natr:.2f}%",
        "SMC_Reasons": "",
        "Deriv_Reasons": f"funding {_funding_rate(ticker):.5f}",
        "High_WR_Score": total_score,
        "Move_SL_To_BE_After_TP": int(cfg.get("move_sl_to_be_after_tp", 2)),
        "df": df,
    }


def analyze_high_wr_scalp(
    df: pd.DataFrame,
    ticker: Optional[Dict],
    symbol: str,
    timeframe: str,
    config: Optional[Dict] = None,
    macro_regime: Optional[str] = None,
) -> Optional[Dict]:
    cfg = get_high_wr_config(config)
    if not is_enabled_for_timeframe(timeframe, cfg):
        return None
    allowed = set(cfg.get("allowed_symbols") or [])
    blocked = set(cfg.get("blocked_symbols") or [])
    if allowed and symbol not in allowed:
        return None
    if symbol in blocked:
        return None
    if df is None or len(df) < 80:
        return None

    rvol = _rvol(df)
    adx = _last(df, "ADX_14")
    funding = _funding_rate(ticker)
    regime = macro_regime or detect_regime(df)

    long_score, long_reasons = _score_long(df, rvol, adx, funding, cfg)
    short_score, short_reasons = _score_short(df, rvol, adx, funding, cfg)

    if "Trending Bear" in str(regime):
        long_score -= 2
        long_reasons.append("macro bear penalty")
    if "Trending Bull" in str(regime):
        short_score -= 2
        short_reasons.append("macro bull penalty")

    allow_longs = bool(cfg.get("allow_longs", True))
    allow_shorts = bool(cfg.get("allow_shorts", True))
    if not allow_longs and not allow_shorts:
        return None
    if not allow_longs:
        long_score = -999
    if not allow_shorts:
        short_score = -999

    if long_score >= short_score:
        side, score, reasons = "Long", long_score, long_reasons
    else:
        side, score, reasons = "Short", short_score, short_reasons

    if score < int(cfg.get("min_score", 7)):
        return None

    reasons.append(f"regime {regime}")
    return _build_signal(symbol, timeframe, df, ticker, side, score, reasons, cfg)
