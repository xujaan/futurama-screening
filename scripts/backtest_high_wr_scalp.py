#!/usr/bin/env python3
"""
Backtest HIGH_WR_SCALP on public OHLCV data.

No private API keys are used. The strategy simulates manual execution:
wait for entry zone, partial targets, and move stop loss to breakeven after TP2.
"""

import argparse
import csv
import os
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

import ccxt
import pandas as pd

from modules.config_loader import CONFIG
from modules.high_wr_scalp import analyze_high_wr_scalp, get_high_wr_config
from modules.technicals import get_technicals


@dataclass
class TradeResult:
    symbol: str
    timeframe: str
    signal_time: str
    entry_time: str
    exit_time: str
    side: str
    entry: float
    stop: float
    exit: float
    pnl_pct: float
    risk_pct: float
    rr: float
    tp_hits: int
    outcome: str
    hold_bars: int
    score: int


def timeframe_ms(timeframe: str) -> int:
    unit = timeframe[-1]
    value = int(timeframe[:-1])
    if unit == "m":
        return value * 60_000
    if unit == "h":
        return value * 3_600_000
    if unit == "d":
        return value * 86_400_000
    raise ValueError(f"Unsupported timeframe: {timeframe}")


def make_exchange(name: str):
    klass = getattr(ccxt, name)
    options = {"defaultType": "swap", "adjustForTimeDifference": True}
    if name == "binance":
        options["defaultType"] = "future"
    return klass({"enableRateLimit": True, "options": options})


def resolve_symbol(exchange, raw: str) -> Optional[str]:
    markets = exchange.load_markets()
    candidates = [raw]
    compact = raw.replace("/", "").replace(":USDT", "").replace("USDT", "")
    candidates.extend([f"{compact}/USDT:USDT", f"{compact}/USDT"])
    for candidate in candidates:
        if candidate in markets:
            return candidate
    return None


def market_quote_volume(ticker: Dict) -> float:
    for key in ("quoteVolume",):
        value = ticker.get(key)
        if value:
            try:
                return float(value)
            except Exception:
                pass
    info = ticker.get("info", {}) if isinstance(ticker, dict) else {}
    for key in ("turnover24h", "quoteVolume", "turnover"):
        value = info.get(key)
        if value:
            try:
                return float(value)
            except Exception:
                pass
    for key in ("baseVolume",):
        value = ticker.get(key)
        if value:
            try:
                return float(value)
            except Exception:
                pass
    for key in ("volume24h",):
        value = info.get(key)
        if value:
            try:
                return float(value)
            except Exception:
                pass
    return 0.0


def list_universe(exchange, max_symbols: int = 0, min_quote_volume: float = 0.0) -> List[str]:
    markets = exchange.load_markets()
    stablecoins = {"USDC", "USDT", "DAI", "FDUSD", "USDD", "USDE", "TUSD", "BUSD", "PYUSD", "USDS", "EUR", "USD"}
    symbols = [
        symbol
        for symbol, market in markets.items()
        if market.get("swap")
        and market.get("type") == "swap"
        and market.get("quote") == "USDT"
        and market.get("active", True)
        and market.get("base") not in stablecoins
    ]

    if max_symbols > 0 or min_quote_volume > 0:
        try:
            tickers = exchange.fetch_tickers(symbols)
            ranked = []
            for symbol in symbols:
                volume = market_quote_volume(tickers.get(symbol, {}))
                if volume >= min_quote_volume:
                    ranked.append((volume, symbol))
            ranked.sort(reverse=True)
            symbols = [symbol for _, symbol in ranked]
        except Exception as exc:
            print(f"volume ranking skipped: {exc}")

    if max_symbols > 0:
        symbols = symbols[:max_symbols]
    return symbols


def fetch_ohlcv(exchange, symbol: str, timeframe: str, days: int) -> pd.DataFrame:
    since = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp() * 1000)
    tf_ms = timeframe_ms(timeframe)
    rows: List[List[float]] = []
    seen = set()

    while True:
        batch = exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=since, limit=1000)
        if not batch:
            break
        new_rows = []
        for row in batch:
            if row[0] not in seen:
                seen.add(row[0])
                new_rows.append(row)
        if not new_rows:
            break
        rows.extend(new_rows)
        since = int(new_rows[-1][0]) + tf_ms
        if len(batch) < 1000 or since >= int(datetime.now(timezone.utc).timestamp() * 1000):
            break
        time.sleep(max(exchange.rateLimit / 1000, 0.05))

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    return df.drop_duplicates("timestamp").reset_index(drop=True)


def load_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    required = {"timestamp", "open", "high", "low", "close", "volume"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"CSV missing columns: {sorted(missing)}")
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df.sort_values("timestamp").reset_index(drop=True)


def net_return(entry: float, exit_price: float, side: str, fee_rate: float, slippage_pct: float) -> float:
    if side == "Long":
        gross = (exit_price - entry) / entry
    else:
        gross = (entry - exit_price) / entry
    return gross - (fee_rate * 2) - (slippage_pct * 2)


def simulate_trade(
    raw_df: pd.DataFrame,
    signal_idx: int,
    signal: Dict,
    cfg: Dict,
    max_hold_bars: int,
    entry_wait_bars: int,
    fee_rate: float,
    slippage_pct: float,
    entry_fill: str,
) -> Optional[TradeResult]:
    side = signal["Side"]
    if entry_fill == "ideal":
        entry = float(signal["Entry"])
    elif side == "Long" and entry_fill == "conservative":
        entry = float(signal["Entry_Low"])
    elif side == "Short" and entry_fill == "conservative":
        entry = float(signal["Entry_High"])
    elif side == "Long":
        entry = float(signal["Entry_High"])
    else:
        entry = float(signal["Entry_Low"])

    if side == "Long":
        entry_touched = lambda row: float(row["low"]) <= entry
        stop_touched = lambda row, stop: float(row["low"]) <= stop
        tp_touched = lambda row, tp: float(row["high"]) >= tp
    else:
        entry_touched = lambda row: float(row["high"]) >= entry
        stop_touched = lambda row, stop: float(row["high"]) >= stop
        tp_touched = lambda row, tp: float(row["low"]) <= tp

    stop = float(signal["SL"])
    risk_pct = abs(entry - stop) / entry * 100
    risk_abs = abs(entry - stop)
    if risk_abs <= 0:
        return None
    rr = abs(float(signal.get("TP_Plan", [{}])[-1].get("price", entry)) - entry) / risk_abs
    tps = [float(tp["price"]) for tp in signal.get("TP_Plan", [])]
    splits = [float(tp["close_ratio"]) for tp in signal.get("TP_Plan", [])]
    if not tps or not splits:
        return None

    entry_idx = None
    wait_end = min(len(raw_df), signal_idx + 1 + entry_wait_bars)
    for idx in range(signal_idx + 1, wait_end):
        if entry_touched(raw_df.iloc[idx]):
            entry_idx = idx
            break
    if entry_idx is None:
        return None

    remaining = 1.0
    pnl_pct = 0.0
    tp_hits = 0
    exit_price = entry
    outcome = "TIME_EXIT"
    move_be_after = int(cfg.get("move_sl_to_be_after_tp", 2))
    max_idx = min(len(raw_df), entry_idx + 1 + max_hold_bars)

    for idx in range(entry_idx + 1, max_idx):
        row = raw_df.iloc[idx]

        if stop_touched(row, stop):
            pnl_pct += remaining * net_return(entry, stop, side, fee_rate, slippage_pct) * 100
            remaining = 0.0
            exit_price = stop
            outcome = "SL" if tp_hits < move_be_after else "BE_OR_TRAIL"
            exit_idx = idx
            break

        while tp_hits < len(tps) and tp_touched(row, tps[tp_hits]):
            close_ratio = min(splits[tp_hits], remaining)
            pnl_pct += close_ratio * net_return(entry, tps[tp_hits], side, fee_rate, slippage_pct) * 100
            remaining -= close_ratio
            exit_price = tps[tp_hits]
            tp_hits += 1
            if tp_hits >= move_be_after:
                stop = entry
            if remaining <= 1e-9:
                outcome = "FULL_TP"
                exit_idx = idx
                break
        if remaining <= 1e-9:
            break
    else:
        exit_idx = max_idx - 1
        close = float(raw_df.iloc[exit_idx]["close"])
        pnl_pct += remaining * net_return(entry, close, side, fee_rate, slippage_pct) * 100
        exit_price = close

    if outcome == "TIME_EXIT":
        if pnl_pct > 0:
            outcome = "TIME_WIN"
        elif pnl_pct < 0:
            outcome = "TIME_LOSS"
        else:
            outcome = "FLAT"

    return TradeResult(
        symbol=signal["Symbol"],
        timeframe=signal["Timeframe"],
        signal_time=str(raw_df.iloc[signal_idx]["timestamp"]),
        entry_time=str(raw_df.iloc[entry_idx]["timestamp"]),
        exit_time=str(raw_df.iloc[exit_idx]["timestamp"]),
        side=side,
        entry=entry,
        stop=float(signal["SL"]),
        exit=exit_price,
        pnl_pct=pnl_pct,
        risk_pct=risk_pct,
        rr=rr,
        tp_hits=tp_hits,
        outcome=outcome,
        hold_bars=exit_idx - entry_idx,
        score=int(signal.get("High_WR_Score", 0)),
    )


def prepare_technical_df(df: pd.DataFrame) -> pd.DataFrame:
    return get_technicals(df.copy()).reset_index(drop=True)


def nearest_position_by_time(df: pd.DataFrame, timestamp) -> int:
    ts = pd.Timestamp(timestamp)
    matches = df.index[df["timestamp"] == ts]
    if len(matches):
        return int(matches[0])
    pos = df["timestamp"].searchsorted(ts, side="left")
    return int(max(0, min(pos, len(df) - 1)))


def backtest_symbol(
    symbol: str,
    df: pd.DataFrame,
    timeframe: str,
    cfg: Dict,
    max_hold_bars: int,
    entry_wait_bars: int,
    fee_rate: float,
    slippage_pct: float,
    entry_fill: str,
    start_idx: Optional[int] = None,
    end_idx: Optional[int] = None,
) -> List[TradeResult]:
    results: List[TradeResult] = []
    tech_df = prepare_technical_df(df)
    if len(tech_df) < 240:
        return results

    raw_start = max(240, int(start_idx or 240))
    raw_stop = min(int(end_idx or len(df)), len(df))
    tech_i = nearest_position_by_time(tech_df, df.iloc[raw_start]["timestamp"])
    tech_stop = nearest_position_by_time(tech_df, df.iloc[max(raw_start, raw_stop - 1)]["timestamp"])
    tech_stop = min(tech_stop, len(tech_df) - 1)

    while tech_i < tech_stop:
        signal_ts = tech_df.iloc[tech_i]["timestamp"]
        raw_i = nearest_position_by_time(df, signal_ts)
        if raw_i >= raw_stop - max_hold_bars - entry_wait_bars - 2:
            break
        if tech_i < 80:
            tech_i += 1
            continue

        tech = tech_df.iloc[: tech_i + 1]
        last = tech.iloc[-1]
        if (
            float(last.get("volume", 0)) <= 0
            or pd.isna(last.get("ATR_14"))
            or pd.isna(last.get("ADX_14"))
            or pd.isna(last.get("EMA_Fast"))
            or pd.isna(last.get("SMA_50"))
        ):
            tech_i += 1
            continue

        ticker = {"last": float(last["close"]), "info": {"fundingRate": "0"}}
        signal = analyze_high_wr_scalp(tech, ticker, symbol, timeframe, cfg)
        if not signal:
            tech_i += 1
            continue

        trade = simulate_trade(
            df,
            raw_i,
            signal,
            cfg,
            max_hold_bars,
            entry_wait_bars,
            fee_rate,
            slippage_pct,
            entry_fill,
        )
        if trade:
            results.append(trade)
            exit_ts = pd.Timestamp(trade.exit_time)
            tech_i = nearest_position_by_time(tech_df, exit_ts) + 1
        else:
            tech_i += 1
    return results


def summarize(results: Iterable[TradeResult]) -> Dict:
    rows = list(results)
    if not rows:
        return {"trades": 0}

    pnl = [r.pnl_pct for r in rows]
    wins = [x for x in pnl if x > 0]
    losses = [x for x in pnl if x < 0]
    gross_win = sum(wins)
    gross_loss = abs(sum(losses))
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for value in pnl:
        equity += value
        peak = max(peak, equity)
        max_dd = min(max_dd, equity - peak)

    return {
        "trades": len(rows),
        "win_rate": (len(wins) / len(rows)) * 100,
        "tp1_rate": (sum(1 for r in rows if r.tp_hits >= 1) / len(rows)) * 100,
        "tp2_rate": (sum(1 for r in rows if r.tp_hits >= 2) / len(rows)) * 100,
        "avg_pnl_pct": sum(pnl) / len(pnl),
        "total_pnl_pct": sum(pnl),
        "profit_factor": gross_win / gross_loss if gross_loss > 0 else float("inf"),
        "max_drawdown_pct": max_dd,
    }


def print_summary(summary: Dict, results: List[TradeResult]) -> None:
    if not results:
        print("No trades generated. Loosen filters or use more history.")
        return

    print("HIGH_WR_SCALP backtest")
    print(f"trades          : {summary['trades']}")
    print(f"win_rate        : {summary['win_rate']:.2f}%")
    print(f"tp1_rate        : {summary['tp1_rate']:.2f}%")
    print(f"tp2_rate        : {summary['tp2_rate']:.2f}%")
    print(f"avg_pnl_pct     : {summary['avg_pnl_pct']:.4f}%")
    print(f"total_pnl_pct   : {summary['total_pnl_pct']:.4f}%")
    print(f"profit_factor   : {summary['profit_factor']:.2f}")
    print(f"max_drawdown_pct: {summary['max_drawdown_pct']:.4f}%")
    print()
    by_symbol: Dict[str, List[TradeResult]] = {}
    for row in results:
        by_symbol.setdefault(row.symbol, []).append(row)
    for symbol, rows in sorted(by_symbol.items()):
        local = summarize(rows)
        print(
            f"{symbol:18} trades={local['trades']:3} "
            f"wr={local['win_rate']:6.2f}% "
            f"avg={local['avg_pnl_pct']:8.4f}% "
            f"pf={local['profit_factor']:5.2f}"
        )


def write_csv(path: str, results: List[TradeResult]) -> None:
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(asdict(results[0]).keys()))
        writer.writeheader()
        for row in results:
            writer.writerow(asdict(row))


def optimization_presets(base_cfg: Dict) -> List[tuple[str, Dict]]:
    presets = [
        (
            "configured_default",
            {},
        ),
        (
            "fast_be",
            {
                "entry_atr_near": 0.20,
                "entry_atr_far": 0.80,
                "sl_atr": 0.65,
                "tp_atr_multipliers": [0.75, 1.15, 1.65, 2.20, 2.90, 3.80],
                "max_sl_pct": 1.00,
                "min_tp2_r": 0.95,
                "min_runner_r": 2.35,
                "min_score": 8,
            },
        ),
        (
            "patient_pullback",
            {
                "entry_atr_near": 0.35,
                "entry_atr_far": 1.20,
                "sl_atr": 0.70,
                "tp_atr_multipliers": [0.90, 1.40, 2.00, 2.70, 3.50, 4.50],
                "max_sl_pct": 1.15,
                "min_tp2_r": 1.00,
                "min_runner_r": 2.50,
                "min_score": 8,
            },
        ),
        (
            "quality_only",
            {
                "entry_atr_near": 0.25,
                "entry_atr_far": 1.00,
                "sl_atr": 0.70,
                "tp_atr_multipliers": [0.90, 1.35, 1.90, 2.60, 3.40, 4.30],
                "max_sl_pct": 1.10,
                "min_tp2_r": 1.00,
                "min_runner_r": 2.75,
                "min_score": 9,
            },
        ),
        (
            "balanced_looser",
            {
                "entry_atr_near": 0.25,
                "entry_atr_far": 0.95,
                "sl_atr": 0.85,
                "tp_atr_multipliers": [0.80, 1.20, 1.70, 2.30, 3.00, 4.00],
                "max_sl_pct": 1.35,
                "min_tp2_r": 0.85,
                "min_runner_r": 2.00,
                "min_score": 8,
            },
        ),
    ]

    built = []
    for name, overrides in presets:
        cfg = base_cfg.copy()
        cfg.update(overrides)
        built.append((name, cfg))
    return built


def run_backtest_set(
    datasets: List[tuple[str, pd.DataFrame]],
    timeframe: str,
    cfg: Dict,
    max_hold_bars: int,
    entry_wait_bars: int,
    fee_rate: float,
    slippage_pct: float,
    entry_fill: str,
    ranges: Optional[Dict[str, tuple[Optional[int], Optional[int]]]] = None,
    progress_label: Optional[str] = None,
) -> List[TradeResult]:
    all_results: List[TradeResult] = []
    total = len(datasets)
    for idx, (symbol, df) in enumerate(datasets, start=1):
        if progress_label and (idx == 1 or idx % 20 == 0 or idx == total):
            print(f"{progress_label}: {idx}/{total} {symbol}", flush=True)
        start_idx, end_idx = (ranges or {}).get(symbol, (None, None))
        all_results.extend(
            backtest_symbol(
                symbol,
                df,
                timeframe,
                cfg,
                max_hold_bars,
                entry_wait_bars,
                fee_rate,
                slippage_pct,
                entry_fill,
                start_idx,
                end_idx,
            )
        )
    return all_results


def summarize_by_symbol(results: List[TradeResult]) -> Dict[str, Dict]:
    grouped: Dict[str, List[TradeResult]] = {}
    for row in results:
        grouped.setdefault(row.symbol, []).append(row)
    return {symbol: summarize(rows) for symbol, rows in grouped.items()}


def group_by_symbol(results: List[TradeResult]) -> Dict[str, List[TradeResult]]:
    grouped: Dict[str, List[TradeResult]] = {}
    for row in results:
        grouped.setdefault(row.symbol, []).append(row)
    return grouped


def build_walk_forward_ranges(datasets: List[tuple[str, pd.DataFrame]], train_ratio: float) -> tuple[Dict, Dict]:
    train_ranges, test_ranges = {}, {}
    train_ratio = min(max(float(train_ratio), 0.10), 0.90)
    for symbol, df in datasets:
        split_idx = max(240, min(len(df) - 2, int(len(df) * train_ratio)))
        train_ranges[symbol] = (240, split_idx)
        test_ranges[symbol] = (split_idx, len(df))
    return train_ranges, test_ranges


def screen_stability(
    rows: List[TradeResult],
    slices: int,
    min_slice_pf: float,
    min_slice_avg_pct: float,
) -> Dict:
    if slices <= 1:
        return {"active_slices": 0, "positive_slices": 0, "detail": ""}
    if not rows:
        return {"active_slices": 0, "positive_slices": 0, "detail": ""}

    ordered = sorted(rows, key=lambda row: pd.Timestamp(row.signal_time))
    start = pd.Timestamp(ordered[0].signal_time)
    end = pd.Timestamp(ordered[-1].signal_time)
    span = max((end - start).total_seconds(), 1.0)
    buckets: List[List[TradeResult]] = [[] for _ in range(slices)]

    for row in ordered:
        offset = (pd.Timestamp(row.signal_time) - start).total_seconds()
        bucket_idx = min(slices - 1, int((offset / span) * slices))
        buckets[bucket_idx].append(row)

    active = 0
    positive = 0
    parts = []
    for idx, bucket in enumerate(buckets, start=1):
        if not bucket:
            parts.append(f"{idx}:n0")
            continue
        active += 1
        summary = summarize(bucket)
        pf = float(summary.get("profit_factor", 0.0))
        avg = float(summary.get("avg_pnl_pct", 0.0))
        total = float(summary.get("total_pnl_pct", 0.0))
        if pf >= min_slice_pf and avg >= min_slice_avg_pct and total > 0:
            positive += 1
        parts.append(f"{idx}:n{summary.get('trades', 0)}/pf{pf:.2f}/avg{avg:.4f}")

    return {
        "active_slices": active,
        "positive_slices": positive,
        "detail": ";".join(parts),
    }


def screen_symbol_diagnostics(train_results: List[TradeResult], args) -> Dict[str, Dict]:
    grouped = group_by_symbol(train_results)
    summaries = summarize_by_symbol(train_results)
    diagnostics = {}
    for symbol, summary in summaries.items():
        stability = screen_stability(
            grouped.get(symbol, []),
            args.screen_slices,
            args.min_slice_pf,
            args.min_slice_avg_pct,
        )
        reason = ""
        if summary.get("trades", 0) < args.min_screen_trades:
            reason = "low_trades"
        elif summary.get("profit_factor", 0.0) < args.min_screen_pf:
            reason = "low_pf"
        elif summary.get("total_pnl_pct", 0.0) < args.min_screen_total_pct:
            reason = "low_total"
        elif summary.get("win_rate", 0.0) < args.min_screen_wr:
            reason = "low_wr"
        elif summary.get("avg_pnl_pct", 0.0) < args.min_screen_avg_pct:
            reason = "low_avg"
        elif (
            args.screen_slices > 1
            and stability["positive_slices"] < args.min_positive_slices
        ):
            reason = "unstable_slices"
        diagnostics[symbol] = {
            "summary": summary,
            "stability": stability,
            "reason": reason,
        }
    return diagnostics


def selected_symbols_from_screen(
    train_results: List[TradeResult],
    min_trades: int,
    min_pf: float,
    min_total_pct: float,
    min_wr: float,
    min_avg_pct: float,
    max_symbols: int,
    diagnostics: Optional[Dict[str, Dict]] = None,
    min_positive_slices: int = 1,
) -> List[str]:
    summaries = summarize_by_symbol(train_results)
    candidates = []
    for symbol, summary in summaries.items():
        if summary.get("trades", 0) < min_trades:
            continue
        if summary.get("profit_factor", 0.0) < min_pf:
            continue
        if summary.get("total_pnl_pct", 0.0) < min_total_pct:
            continue
        if summary.get("win_rate", 0.0) < min_wr:
            continue
        if summary.get("avg_pnl_pct", 0.0) < min_avg_pct:
            continue
        stability = (diagnostics or {}).get(symbol, {}).get("stability", {})
        positive_slices = int(stability.get("positive_slices", 0))
        if diagnostics and positive_slices < min_positive_slices:
            continue
        rank = (
            positive_slices,
            summary.get("profit_factor", 0.0),
            summary.get("avg_pnl_pct", 0.0),
            summary.get("win_rate", 0.0),
            summary.get("total_pnl_pct", 0.0),
        )
        candidates.append((rank, symbol))
    candidates.sort(reverse=True)
    symbols = [symbol for _, symbol in candidates]
    if max_symbols > 0:
        symbols = symbols[:max_symbols]
    return sorted(symbols)


def write_screen_report(
    path: str,
    rows: List[Dict],
) -> None:
    if not rows:
        return
    fieldnames = [
        "preset",
        "symbol",
        "selected",
        "reject_reason",
        "train_trades",
        "train_wr",
        "train_pf",
        "train_avg",
        "train_total",
        "stable_positive_slices",
        "stable_active_slices",
        "stable_detail",
        "test_trades",
        "test_wr",
        "test_pf",
        "test_avg",
        "test_total",
    ]
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def print_screen_table(
    selected: List[str],
    train_results: List[TradeResult],
    test_results: List[TradeResult],
    max_rows: int = 30,
) -> None:
    if not selected:
        return
    train_by = summarize_by_symbol(train_results)
    test_by = summarize_by_symbol(test_results)
    print("  symbol train/test:")
    for symbol in selected[:max_rows]:
        tr = train_by.get(symbol, {"trades": 0})
        te = test_by.get(symbol, {"trades": 0})
        print(
            f"  {symbol:18} "
            f"train n={tr.get('trades', 0):3} wr={tr.get('win_rate', 0):6.2f}% "
            f"pf={tr.get('profit_factor', 0):5.2f} avg={tr.get('avg_pnl_pct', 0):8.4f}% | "
            f"test n={te.get('trades', 0):3} wr={te.get('win_rate', 0):6.2f}% "
            f"pf={te.get('profit_factor', 0):5.2f} avg={te.get('avg_pnl_pct', 0):8.4f}%"
        )
    if len(selected) > max_rows:
        print(f"  ... {len(selected) - max_rows} more")


def run_screen_then_test(
    datasets: List[tuple[str, pd.DataFrame]],
    timeframe: str,
    cfg: Dict,
    args,
) -> tuple[str, Dict, Dict, List[TradeResult]]:
    train_ranges, test_ranges = build_walk_forward_ranges(datasets, args.train_ratio)
    presets = optimization_presets(cfg) if args.optimize else [("configured", cfg)]
    scored = []
    screen_report_rows: List[Dict] = []

    print("Screen all symbols, then holdout test")
    print(
        f"universe={len(datasets)} train_ratio={args.train_ratio:.2f} "
        f"min_trades={args.min_screen_trades} min_pf={args.min_screen_pf} "
        f"min_wr={args.min_screen_wr} min_avg={args.min_screen_avg_pct} "
        f"min_total={args.min_screen_total_pct} "
        f"screen_slices={args.screen_slices} min_positive_slices={args.min_positive_slices} "
        f"max_selected={args.max_selected_symbols}"
    )

    for name, test_cfg in presets:
        train_results = run_backtest_set(
            datasets,
            timeframe,
            test_cfg,
            args.max_hold_bars,
            args.entry_wait_bars,
            args.fee_rate,
            args.slippage_pct,
            args.entry_fill,
            train_ranges,
            f"{name} train",
        )
        diagnostics = screen_symbol_diagnostics(train_results, args)
        active_diagnostics = diagnostics if args.screen_slices > 1 else None
        selected = selected_symbols_from_screen(
            train_results,
            args.min_screen_trades,
            args.min_screen_pf,
            args.min_screen_total_pct,
            args.min_screen_wr,
            args.min_screen_avg_pct,
            args.max_selected_symbols,
            active_diagnostics,
            args.min_positive_slices,
        )
        selected_set = set(selected)
        selected_datasets = [(symbol, df) for symbol, df in datasets if symbol in selected_set]
        test_results = run_backtest_set(
            selected_datasets,
            timeframe,
            test_cfg,
            args.max_hold_bars,
            args.entry_wait_bars,
            args.fee_rate,
            args.slippage_pct,
            args.entry_fill,
            test_ranges,
            f"{name} test",
        )
        train_summary = summarize([row for row in train_results if row.symbol in selected_set])
        test_summary = summarize(test_results)
        scored.append((name, test_cfg, selected, train_summary, test_summary, test_results))

        if args.screen_report:
            train_by = summarize_by_symbol(train_results)
            test_by = summarize_by_symbol(test_results)
            for symbol, _ in datasets:
                train_stats = train_by.get(symbol, {"trades": 0})
                test_stats = test_by.get(symbol, {"trades": 0})
                diag = diagnostics.get(
                    symbol,
                    {
                        "stability": {"positive_slices": 0, "active_slices": 0, "detail": ""},
                        "reason": "low_trades",
                    },
                )
                screen_report_rows.append(
                    {
                        "preset": name,
                        "symbol": symbol,
                        "selected": symbol in selected_set,
                        "reject_reason": "" if symbol in selected_set else diag.get("reason", "low_trades"),
                        "train_trades": train_stats.get("trades", 0),
                        "train_wr": train_stats.get("win_rate", 0.0),
                        "train_pf": train_stats.get("profit_factor", 0.0),
                        "train_avg": train_stats.get("avg_pnl_pct", 0.0),
                        "train_total": train_stats.get("total_pnl_pct", 0.0),
                        "stable_positive_slices": diag["stability"].get("positive_slices", 0),
                        "stable_active_slices": diag["stability"].get("active_slices", 0),
                        "stable_detail": diag["stability"].get("detail", ""),
                        "test_trades": test_stats.get("trades", 0),
                        "test_wr": test_stats.get("win_rate", 0.0),
                        "test_pf": test_stats.get("profit_factor", 0.0),
                        "test_avg": test_stats.get("avg_pnl_pct", 0.0),
                        "test_total": test_stats.get("total_pnl_pct", 0.0),
                    }
                )

        if selected:
            print(
                f"{name:16} selected={len(selected):3} "
                f"train_pf={train_summary.get('profit_factor', 0):5.2f} "
                f"train_total={train_summary.get('total_pnl_pct', 0):8.4f}% "
                f"test_trades={test_summary.get('trades', 0):4} "
                f"test_wr={test_summary.get('win_rate', 0):6.2f}% "
                f"test_avg={test_summary.get('avg_pnl_pct', 0):8.4f}% "
                f"test_total={test_summary.get('total_pnl_pct', 0):8.4f}% "
                f"test_pf={test_summary.get('profit_factor', 0):5.2f}"
            )
            print(f"  selected: {', '.join(selected[:30])}{' ...' if len(selected) > 30 else ''}")
            print_screen_table(selected, train_results, test_results)
        else:
            print(f"{name:16} selected=  0")

    if args.screen_report and screen_report_rows:
        write_screen_report(args.screen_report, screen_report_rows)
        print(f"wrote screen report {args.screen_report}")

    valid = [item for item in scored if item[4].get("trades", 0)]
    if not valid:
        return "", cfg, {"trades": 0}, []

    if args.best_preset_by == "holdout":
        best = max(valid, key=lambda item: (item[4].get("profit_factor", 0), item[4].get("total_pnl_pct", 0)))
        best_label = "holdout"
    else:
        best = max(valid, key=lambda item: (item[3].get("profit_factor", 0), item[3].get("total_pnl_pct", 0)))
        best_label = "train screen"
    name, best_cfg, selected, _, test_summary, test_results = best
    print()
    print(f"Best preset by {best_label}: {name}")
    print(f"Selected symbols: {', '.join(selected)}")
    return name, best_cfg, test_summary, test_results


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--exchange", default="bybit", choices=["bybit", "binance", "bitget"])
    parser.add_argument("--symbols", default="DUSK,RESOLV,LUMIA,UB,GWEI,TRADOOR,ENSO")
    parser.add_argument("--all-symbols", action="store_true")
    parser.add_argument("--max-symbols", type=int, default=0)
    parser.add_argument("--min-quote-volume", type=float, default=0.0)
    parser.add_argument("--timeframe", default="15m")
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--csv")
    parser.add_argument("--csv-symbol", default="CSV/USDT:USDT")
    parser.add_argument("--max-hold-bars", type=int, default=32)
    parser.add_argument("--entry-wait-bars", type=int, default=8)
    parser.add_argument("--fee-rate", type=float, default=0.0006)
    parser.add_argument("--slippage-pct", type=float, default=0.0003)
    parser.add_argument("--entry-fill", choices=["ideal", "aggressive", "conservative"], default="ideal")
    parser.add_argument("--optimize", action="store_true")
    parser.add_argument("--screen-then-test", action="store_true")
    parser.add_argument("--train-ratio", type=float, default=0.5)
    parser.add_argument("--min-screen-trades", type=int, default=3)
    parser.add_argument("--min-screen-pf", type=float, default=1.35)
    parser.add_argument("--min-screen-total-pct", type=float, default=0.0)
    parser.add_argument("--min-screen-wr", type=float, default=55.0)
    parser.add_argument("--min-screen-avg-pct", type=float, default=0.03)
    parser.add_argument("--max-selected-symbols", type=int, default=20)
    parser.add_argument("--screen-slices", type=int, default=1)
    parser.add_argument("--min-positive-slices", type=int, default=1)
    parser.add_argument("--min-slice-pf", type=float, default=1.0)
    parser.add_argument("--min-slice-avg-pct", type=float, default=0.0)
    parser.add_argument("--best-preset-by", choices=["train", "holdout"], default="holdout")
    parser.add_argument("--screen-report")
    parser.add_argument("--output")
    args = parser.parse_args()

    cfg = get_high_wr_config(CONFIG.get("high_wr_scalp", {}))
    cfg["enabled"] = True
    cfg["timeframes"] = [args.timeframe]

    datasets: List[tuple[str, pd.DataFrame]] = []
    if args.csv:
        df = load_csv(args.csv)
        datasets.append((args.csv_symbol, df))
    else:
        exchange = make_exchange(args.exchange)
        if args.all_symbols:
            raw_symbols = list_universe(exchange, args.max_symbols, args.min_quote_volume)
            print(f"universe loaded: {len(raw_symbols)} symbols")
        else:
            raw_symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]

        for raw in raw_symbols:
            try:
                symbol = raw if "/" in raw else resolve_symbol(exchange, raw)
                if not symbol:
                    print(f"skip {raw}: symbol not found on {args.exchange}")
                    continue
                print(f"fetch {symbol} {args.timeframe} {args.days}d")
                df = fetch_ohlcv(exchange, symbol, args.timeframe, args.days)
            except Exception as exc:
                print(f"skip {raw}: fetch failed: {exc}")
                continue
            if df.empty:
                print(f"skip {symbol}: no candles")
                continue
            datasets.append((symbol, df))

    if args.screen_then_test:
        _, _, summary, all_results = run_screen_then_test(datasets, args.timeframe, cfg, args)
    elif args.optimize:
        scored = []
        print("Optimization presets")
        for name, test_cfg in optimization_presets(cfg):
            results = run_backtest_set(
                datasets,
                args.timeframe,
                test_cfg,
                args.max_hold_bars,
                args.entry_wait_bars,
                args.fee_rate,
                args.slippage_pct,
                args.entry_fill,
            )
            summary = summarize(results)
            scored.append((name, test_cfg, summary, results))
            if summary.get("trades", 0):
                print(
                    f"{name:16} trades={summary['trades']:3} "
                    f"wr={summary['win_rate']:6.2f}% "
                    f"avg={summary['avg_pnl_pct']:8.4f}% "
                    f"total={summary['total_pnl_pct']:8.4f}% "
                    f"pf={summary['profit_factor']:5.2f} "
                    f"dd={summary['max_drawdown_pct']:8.4f}%"
                )
            else:
                print(f"{name:16} trades=  0")

        valid = [item for item in scored if item[2].get("trades", 0)]
        if not valid:
            print("No preset generated trades.")
            return 0
        best = max(valid, key=lambda item: (item[2]["profit_factor"], item[2]["total_pnl_pct"]))
        name, best_cfg, summary, all_results = best
        print()
        print(f"Best preset: {name}")
        print(f"Config overrides: {best_cfg}")
    else:
        all_results = run_backtest_set(
            datasets,
            args.timeframe,
            cfg,
            args.max_hold_bars,
            args.entry_wait_bars,
            args.fee_rate,
            args.slippage_pct,
            args.entry_fill,
        )

    summary = summarize(all_results)
    print_summary(summary, all_results)
    if args.output and all_results:
        write_csv(args.output, all_results)
        print(f"wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
