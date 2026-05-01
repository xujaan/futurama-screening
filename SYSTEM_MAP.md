# Project Summary

- **Tujuan Aplikasi:** Futurabot merupakan bot trading kuantitatif terautomasi lintas bursa (Binance, Bitget, Bybit) yang memindai pair koin (screening), analisa teknikal/SMC, cross-platform polling posisitions, mengambil posisi (long/short), dan sinkronisasi manajemen risiko secara langsung via control Telegram.
- **Tech Stack Utama:**
  - **Runtime & Language:** Python (Scheduler, Threading)
  - **Exchange/Data API:** `ccxt` (Generalised cross-platform execution & polling)
  - **Data Processing:** `pandas`, `pandas_ta_classic`
  - **Database:** Memakai eksklusif `sqlite3` tanpa overhead database eksternal.
  - **Web Dashboard:** Streamlit
  - **Integrasi Eksternal:** Murni Telegram Bot API (Berbahasa Inggris)
- **Pola Arsitektur:** Implementasi sistem terpisah (Decoupled & Event-driven):
  1. _Scanner & Analyzer_ berjalan dengan interval Cron di `main.py`. Base CCXT menyesuaikan `active_cex`. Mode manual `HIGH_WR_SCALP` aktif di timeframe kecil untuk entry-zone, partial TP, dan BE rule.
  2. _Live Executor / Rest Polling Engine_ berjalan terus menerus di `auto_trades.py` dengan fallback websocket parsial pada instansiasi tertentu, ditambah algoritma ATR Position Sizing dan ATR Dynamic Trailing Stop.
  3. Modul _Analyzer_ dipecah layer-by-layer (Technicals, Quant, SMC, Pattern). Melibatkan _Multi-Timeframe Confluence_ untuk filter market regime makro.

# Core Logic Flow (Function-Level Flowchart)

**1. Alur Scanning & Penghasil Sinyal (Cron-based):**
`main.py[scan]` (Load Active CEX) -> `main.py[analyze_ticker]` -> `modules.technicals[get_technicals]` -> `modules.patterns[find_pattern]` -> `modules.smc[analyze_smc]` -> `modules.quant[calculate_metrics]` -> `modules.derivative[analyze_derivatives]` -> `modules.bot[send_alert]`(Telegram Only) -> `modules.execution[execute_entry]` (Opsional jika auto_trade menyala via Telegram).

**High-WR Manual Scalp Path:**
`main.py[analyze_ticker]` -> `modules.high_wr_scalp[analyze_high_wr_scalp]` -> `modules.bot[send_alert]` dengan entry zone, 6 partial TP, dan instruksi move SL to breakeven after TP2. Jalur ini dirancang untuk manual trade; tidak membutuhkan `auto_trades.py`.

**2. Alur Eksekusi Trading & Real-Time Engine:**

- _Pengambilan Sinyal:_ `auto_trades.py[ingest_fresh_signals]` (Baca DB, Validasi Leverage per platform CCXT).
- _Eksekusi Antrean:_ `auto_trades.py[execute_pending_orders]` (Market Limit check) -> API CEX via `ccxt`.
- _Live Monitoring & TP/SL Automation:_
  - (Binance/Bitget CEX): `auto_trades.py[ccxt_poll_positions]` CCXT Polling Loop merotasi data position dari REST, menaruh Chandelier Trailing Stop, dan limit Take Profit secara algoritmik.
  - (Bybit): Dipertahankan mengkonsumsi callback Websocket real-time melalui utilitas engine di `sync_active_exchange()`.
- Telegram Switcher: Telegram `/cex` Command -> set DB `active_cex` -> Polling Engine auto re-initiation CCXT.

# Clean Tree

```text
./
├── auto_trades.py
├── futurabot.sqlite
├── config.example.json
├── config.json
├── dashboard.py
├── deploy/
│   ├── bot.service
│   └── restart_bot.sh
├── main.py
├── scripts/
│   └── backtest_high_wr_scalp.py
└── modules/
    ├── __init__.py
    ├── bot.py
    ├── config_loader.py
    ├── database.py
    ├── exchange_manager.py
    ├── high_wr_scalp.py
    ├── derivatives.py
    ├── execution.py
    ├── patterns.py
    ├── quant.py
    ├── smc.py
    ├── technicals.py
    └── telegram_listener.py
```

# Module Map (The Chapters)

- **`main.py`**
  - **Fungsi Utama:** `scan()`, `analyze_ticker()`
  - **Peran:** Entrypoint utama untuk proses memindai pasar paralel, mengambil instance CEX terkini dari `exchange_manager`, dan skoring setup trade.
- **`auto_trades.py`**
  - **Fungsi Utama:** `ingest_fresh_signals()`, `execute_pending_orders()`, `ccxt_poll_positions()`, `sync_active_exchange()`
  - **Peran:** Engine terpisah yang menangani CCXT polling atau websocket untuk lifecycle order (Stop loss breakeven otomatis, Limit TPs, Syncing CEX switch).
- **`dashboard.py`**
  - **Fungsi Utama:** `main()`, `load_data()`
  - **Peran:** Dashboard Streamlit (WebUI) berbahasa Inggris murni memantau performa, metrics trading per CEX, dan log.
- **`modules/exchange_manager.py`**
  - **Fungsi Utama:** `get_current_exchange()`
  - **Peran:** Singleton CEX Node factory yang merespons perubahan setup platform dari Telegram.
- **`modules/database.py`**
  - **Fungsi Utama:** `init_db()`, `get_conn()`, `set_active_cex()`
  - **Peran:** Pure SQLite provider yang mengatur auto-mapping tabel `trades` dan mengelola state parameter telegram `bot_state`.
- **`modules/telegram_listener.py`**
  - **Fungsi Utama:** `TelegramListener (Class)`
  - **Peran:** Pure English command interpreter via long-polling polling untuk mengatur leverage, risk, modal, dan Switch CEX secara real-time.
- **`modules/execution.py`**
  - **Fungsi Utama:** `execute_entry()`, `place_layered_tps()`, `close_position()`
  - **Peran:** Jembatan eksekusi trading generik untuk CCXT terstandardisasi.
- **`modules/high_wr_scalp.py`**
  - **Fungsi Utama:** `analyze_high_wr_scalp()`
  - **Peran:** Generator sinyal manual high win-rate berbasis trend pullback, entry zone ATR, partial targets, dan BE after TP2.
- **`modules/bot.py`**
  - **Fungsi Utama:** `send_alert()`
  - **Peran:** Formatter notifikasi ke Telegram (Termasuk gambar signal chart dan parameter Quant).
- _(Algorithmic Models)_: `technicals.py`, `patterns.py`, `smc.py`, `quant.py`, `derivatives.py` tetap mensuplai raw metric data (OB/RSI/RVOL/Funding dll) ke matrix scoring.
- **`scripts/backtest_high_wr_scalp.py`**
  - **Fungsi Utama:** Fetch OHLCV publik CCXT atau baca CSV, lalu simulasi entry zone, partial TP, fee/slippage, dan BE rule.
  - **Peran:** Validasi offline sebelum mode manual dipakai live.

# Data & Config

- **Lokasi Config:** File konfigurasi berbasis JSON tertulis di `config.json` mendukung Multi-CEX key arraying (`api -> bybit/binance/bitget`).
- **High-WR Config:** `high_wr_scalp` mengatur timeframe `15m`, minimum RVOL/ADX/NATR, entry-zone ATR, SL ATR, target multipliers, split partial close, dan BE trigger.
- **Skema Data Inti:** Ada 3 tabel utama:
  1. `trades`: Tabel pool sinyal hasil scanner algoritmis (symbol, pattern, exit levels).
  2. `active_trades`: Tabel turunan eksekusi real-time per order ID yang aktif (margin, status open/closed). Relasi: `active_trades.signal_id -> trades.id`.
  3. `bot_state`: SQLite persistent store untuk CEX yang aktif (`active_cex`) dan Risk Limits (auto-trade flag, modal, max pairs).
- **Runtime Artifacts:** Memakai database standar lokal `futurabot.sqlite`. PostgreSQL dependency has been utterly stripped untuk keringanan container.

# External Integrations

- **Exchange:** Unified protocol via library **CCXT** yang di instansiasi on-the-fly (`get_current_exchange()`).
- **Sosial/Logs:** Bot Telegram aktif sebagai pusat control hub dan push notifier (`modules/telegram_listener.py`). Discord pipeline telah dibersihkan demi skalabilitas Telegram murni.

# Risks / Blind Spots

- _Polling Delay:_ CCXT Polling `auto_trades.py` diset pada interval pendek namun dapat *slip* (1-2 detik) saat pergerakan flash-crash karena koneksi REST, oleh karena itu *trailing stops* diset mengikuti ATR untuk menyerap *slip* tersebut sebelum Stop Loss tersentuh berlebihan.
- _API Key Boundaries:_ Konfigurasi API `secret` dari CEX lain disatukan di text config.json tanpa enkripsi hardware lokal.
