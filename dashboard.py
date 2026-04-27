import streamlit as st
import pandas as pd
import json
import warnings
warnings.filterwarnings('ignore')

from modules.database import get_conn, release_conn, get_active_cex, init_execution_db
from modules.config_loader import CONFIG
import plotly.express as px

st.set_page_config(page_title="Algorithm Dashboard", page_icon="📈", layout="wide")

def load_data(query, params=None):
    conn = get_conn()
    try:
        df = pd.read_sql_query(query, conn, params=params)
        return df
    except Exception as e:
        st.error(f"Database Error: {e}")
        return pd.DataFrame()
    finally:
        release_conn(conn)

def _sanitize_config(config_dict):
    safe_config = config_dict.copy()
    if 'api' in safe_config:
        safe_api = safe_config['api'].copy()
        for k in ['telegram_bot_token']:
            if k in safe_api and safe_api[k]: safe_api[k] = "********"
        for cx in ['bybit', 'binance', 'bitget']:
            if cx in safe_api and isinstance(safe_api[cx], dict):
                safe_api[cx]['key'] = "********"
                safe_api[cx]['secret'] = "********"
        safe_config['api'] = safe_api
    return safe_config

def main():
    try:
        init_execution_db()
    except Exception as e:
        st.error(f"Execution DB sync failed: {e}")
    active_cex = get_active_cex().upper()
    
    st.sidebar.markdown(f"# ⚡ Algo Dashboard")
    st.sidebar.title("🤖 Quant Bot v8")
    
    st.markdown("""
        <style>
        .metric-container { background: #f0f2f6; border-radius: 8px; padding: 10px; }
        </style>
    """, unsafe_allow_html=True)
    
    st.sidebar.markdown("### 🖥️ Engine Status")
    st.sidebar.markdown(f"**Active CEX:** `{active_cex}`")
    st.sidebar.markdown(f"**Timezone:** `{CONFIG['system']['timezone']}`")
    st.sidebar.markdown(f"**Max Threads:** `{CONFIG['system']['max_threads']}`")
    
    from modules.database import get_risk_config
    try:
        r_cfg = get_risk_config()
        st.sidebar.markdown("### 🚦 Risk Profile")
        st.sidebar.markdown(f"**Auto Trade:** `{'🟢 ON' if r_cfg['auto_trade'] else '🔴 OFF'}`")
        if r_cfg['auto_trade']:
            st.sidebar.markdown(f"**Total Capital:** `${r_cfg['total_trading_capital_usdt']}`")
            st.sidebar.markdown(f"**Max Slots:** `{r_cfg['max_concurrent_trades']} pairs`")
    except: pass

    menu = st.sidebar.radio("Navigation", ["🔴 Live Monitoring", "📋 Trade History", "📊 Analytics", "⚙️ Configuration"])

    if menu == "🔴 Live Monitoring":
        st.title(f"🔴 Live & Waiting Trades ({active_cex})")
        st.markdown("Monitoring board for all orders waiting for entry zone and active positions.")
        
        query = """
            SELECT symbol, side, timeframe, pattern, entry_price, sl_price, tp3 as tp_max, 
            status, tech_score, quant_score, created_at 
            FROM trades 
            WHERE status NOT LIKE '%Closed%' 
            AND status NOT LIKE '%Cancelled%' 
            AND status NOT LIKE '%Stop Loss%'
            ORDER BY created_at DESC
        """
        df = load_data(query)
        
        if not df.empty:
            col1, col2, col3 = st.columns(3)
            col1.metric("📌 Active/Waiting Signals", len(df))
            
            longs = len(df[df['side'] == 'Long'])
            col2.metric("📈 Long / 📉 Short", f"{longs} / {len(df) - longs}")
            
            recent = pd.to_datetime(df['created_at']).max()
            col3.metric("⏱️ Last Signal", recent.strftime('%H:%M:%S') if pd.notnull(recent) else "-")
            
            st.markdown("### 📋 Active Signals Table")
            def color_side(val):
                return 'background-color: rgba(46, 189, 133, 0.2)' if val == 'Long' else 'background-color: rgba(246, 70, 93, 0.2)'
            styled_df = df.style.map(color_side, subset=['side']).format({
                'entry_price': '{:.5f}', 'sl_price': '{:.5f}', 'tp_max': '{:.5f}'
            })
            st.dataframe(styled_df, use_container_width=True, hide_index=True)
        else:
            st.success("🟢 Bot is on standby. No pending limit orders or setups right now.")
            st.markdown("*(Waiting for the next technical scanning cycle...)*")

        st.markdown("### 🧠 Adaptive Trade Management")
        mgmt_query = """
            SELECT symbol, side, strategy, origin_timeframe, status, entry_price, sl_price, tp1,
                   progress_ratio, peak_progress_ratio, locked_profit_level,
                   partial_tp_done, early_exit_done, last_management_note, updated_at
            FROM active_trades
            WHERE status IN ('PENDING', 'OPEN', 'OPEN_TPS_SET')
            ORDER BY updated_at DESC, created_at DESC
        """
        mgmt_df = load_data(mgmt_query)
        if not mgmt_df.empty:
            st.dataframe(
                mgmt_df.style.format({
                    'entry_price': '{:.5f}',
                    'sl_price': '{:.5f}',
                    'tp1': '{:.5f}',
                    'progress_ratio': '{:.2f}',
                    'peak_progress_ratio': '{:.2f}',
                }),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("Belum ada active trade di execution layer.")

    elif menu == "📋 Trade History":
        st.title("📋 Trade History (Closed)")
        
        query = """
            SELECT symbol, side, timeframe, pattern, entry_price, status, closed_at 
            FROM trades 
            WHERE status LIKE '%Closed%' 
            OR status LIKE '%Cancelled%' 
            OR status LIKE '%Stop Loss%' 
            ORDER BY closed_at DESC 
            LIMIT 100
        """
        df = load_data(query)
        
        if not df.empty:
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.download_button(
                label="📥 Download CSV",
                data=df.to_csv(index=False).encode('utf-8'),
                file_name='closed_trades.csv',
                mime='text/csv',
            )
        else:
            st.info("Trade history is currently empty.")
            
    elif menu == "📊 Analytics":
        st.title("📊 Bot Performance Engine")
        
        query_stats = "SELECT status FROM trades WHERE status LIKE '%Closed%' OR status LIKE '%Stop Loss%'"
        df_stats = load_data(query_stats)
        
        if not df_stats.empty:
            win_count = len(df_stats[df_stats['status'].str.contains('TP', case=False, na=False)])
            loss_count = len(df_stats[df_stats['status'].str.contains('Stop Loss', case=False, na=False)])
            total = win_count + loss_count
            
            if total > 0:
                win_rate = (win_count / total) * 100
                st.markdown(f"### Historical Win Rate: **{win_rate:.1f}%**")
                
                fig = px.pie(values=[win_count, loss_count], names=['Take Profit (Win)', 'Stop Loss (Loss)'], 
                             title='Total Win Ratio', color_discrete_sequence=['#2ebd85', '#f6465d'])
                st.plotly_chart(fig)
            else:
                st.warning("No trades have hit TP or SL yet to calculate statistics.")
        else:
            st.info("Performance data is not available yet.")

    elif menu == "⚙️ Configuration":
        st.title("⚙️ Current System Configuration")
        st.info("💡 **Read-Only Mode**: This page is purely for reviewing the running bot's configuration. To change parameters (like Fibonacci, Risk limits, Tokens), modify your `config.json` directly.")
        
        safe_json = _sanitize_config(CONFIG)
        st.json(safe_json)

if __name__ == "__main__":
    main()
