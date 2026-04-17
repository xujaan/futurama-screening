import psycopg2
from psycopg2 import pool
import sqlite3
from modules.config_loader import CONFIG

DB_POOL = None
IS_SQLITE = False

class SQLiteCursorWrapper:
    def __init__(self, cursor):
        self.cursor = cursor

    def execute(self, query, params=None):
        # Convert PostgreSQL parameter bindings
        query = query.replace('%s', '?')
        
        if params is not None:
            self.cursor.execute(query, params)
        else:
            self.cursor.execute(query)
            
        self.description = self.cursor.description
        return self

    def fetchone(self):
        row = self.cursor.fetchone()
        if not row: return None
        if isinstance(row, sqlite3.Row): return dict(row)
        return row
        
    def fetchall(self):
        rows = self.cursor.fetchall()
        if not rows: return []
        if rows and isinstance(rows[0], sqlite3.Row): return [dict(r) for r in rows]
        return rows

    def __getattr__(self, name):
        return getattr(self.cursor, name)

class SQLiteConnWrapper:
    def __init__(self, conn):
        self.conn = conn
    
    def cursor(self, cursor_factory=None):
        if cursor_factory == 'dict':
            self.conn.row_factory = sqlite3.Row
        else:
            self.conn.row_factory = None
        cur = self.conn.cursor()
        return SQLiteCursorWrapper(cur)
        
    def commit(self): self.conn.commit()
    def rollback(self): self.conn.rollback()
    def close(self): self.conn.close()


def init_db():
    global DB_POOL, IS_SQLITE
    db_host = CONFIG['database'].get('host', '').strip()
    
    if not db_host:
        print("⚠️ No Postgres host found. Falling back to SQLite.")
        IS_SQLITE = True
        conn = get_conn()
        migrate_schema(conn)
        release_conn(conn)
        print("✅ SQLite Connected & Schema Synced.")
        return

    try:
        pool_size = CONFIG['system'].get('max_threads', 20) + 5
        DB_POOL = psycopg2.pool.ThreadedConnectionPool(
            minconn=1, maxconn=pool_size,
            host=CONFIG['database']['host'], 
            database=CONFIG['database']['database'],
            user=CONFIG['database']['user'], 
            password=CONFIG['database']['password'],
            port=CONFIG['database']['port']
        )
        IS_SQLITE = False
        conn = DB_POOL.getconn()
        try:
            migrate_schema(conn)
        finally:
            DB_POOL.putconn(conn)
        print("✅ Database Connected & Schema Synced.")
        
    except Exception as e:
        print(f"❌ DB Init Error (Postgres): {e}")
        print("⚠️ Falling back to SQLite due to connection failure.")
        IS_SQLITE = True
        conn = get_conn()
        migrate_schema(conn)
        release_conn(conn)
        print("✅ SQLite Connected & Schema Synced.")

def get_conn():
    global IS_SQLITE, DB_POOL
    if IS_SQLITE:
        conn = sqlite3.connect('bybit_bot.sqlite', check_same_thread=False)
        return SQLiteConnWrapper(conn)
        
    if not DB_POOL: init_db()
    
    if IS_SQLITE:
        conn = sqlite3.connect('bybit_bot.sqlite', check_same_thread=False)
        return SQLiteConnWrapper(conn)
        
    return DB_POOL.getconn()

def release_conn(conn):
    if IS_SQLITE:
        conn.close()
    elif DB_POOL and not IS_SQLITE:
        try:
            if isinstance(conn, SQLiteConnWrapper):
                conn.close()
            else:
                DB_POOL.putconn(conn)
        except: pass

def get_dict_cursor(conn):
    if IS_SQLITE:
        return conn.cursor(cursor_factory='dict')
    else:
        import psycopg2.extras
        return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

def migrate_schema(conn):
    cur = conn.cursor()
    
    required_columns = {
        "id": "SERIAL PRIMARY KEY",
        "symbol": "VARCHAR(100)", 
        "side": "VARCHAR(10)", 
        "timeframe": "VARCHAR(5)", 
        "pattern": "VARCHAR(50)",
        "entry_price": "DECIMAL", 
        "sl_price": "DECIMAL", 
        "tp1": "DECIMAL", "tp2": "DECIMAL", "tp3": "DECIMAL",
        "rr": "DECIMAL",
        "status": "VARCHAR(50) DEFAULT 'Waiting Entry'", 
        "reason": "TEXT",
        "tech_score": "INT", 
        "quant_score": "INT", 
        "deriv_score": "INT", 
        "smc_score": "INT DEFAULT 0",
        "z_score": "DECIMAL DEFAULT 0", 
        "zeta_score": "DECIMAL DEFAULT 0", 
        "obi": "DECIMAL DEFAULT 0",
        "basis": "DECIMAL", 
        "btc_bias": "VARCHAR(50)",
        "tech_reasons": "TEXT",
        "quant_reasons": "TEXT",
        "deriv_reasons": "TEXT",
        "smc_reasons": "TEXT",
        "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP", 
        "entry_hit_at": "TIMESTAMP", 
        "closed_at": "TIMESTAMP", 
        "exit_price": "DECIMAL", 
        "message_id": "VARCHAR(50)", 
        "channel_id": "VARCHAR(50)"
    }

    sqlite_columns = {k: v.replace("SERIAL PRIMARY KEY", "INTEGER PRIMARY KEY AUTOINCREMENT") for k, v in required_columns.items()}
    columns_to_use = sqlite_columns if IS_SQLITE else required_columns

    try:
        if IS_SQLITE:
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='trades';")
            table_exists = cur.fetchone() is not None
        else:
            cur.execute("SELECT to_regclass('public.trades');")
            res = cur.fetchone()
            table_exists = res is not None and res[0] is not None
            
        if not table_exists:
            print("🆕 Table 'trades' not found. Creating fresh...")
            cols = [f"{k} {v}" for k, v in columns_to_use.items()]
            query = f"CREATE TABLE trades ({', '.join(cols)});"
            cur.execute(query)
            print("✅ Table 'trades' created successfully.")
            
        else:
            print("🔍 Checking 'trades' schema for missing columns...")
            if IS_SQLITE:
                cur.execute("PRAGMA table_info('trades');")
                existing_cols = {row['name'] if isinstance(row, dict) else row[1] for row in cur.fetchall()}
            else:
                cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'trades';")
                existing_cols = {row[0] if not isinstance(row, dict) else row['column_name'] for row in cur.fetchall()}
                
            missing_cols = []
            for col, dtype in columns_to_use.items():
                if col not in existing_cols:
                    clean_type = dtype.replace("SERIAL PRIMARY KEY", "INT").replace("PRIMARY KEY", "").replace("INTEGER PRIMARY KEY AUTOINCREMENT", "INTEGER")
                    missing_cols.append(col + " " + clean_type)

            if missing_cols:
                if IS_SQLITE:
                    for mc in missing_cols:
                        cur.execute(f"ALTER TABLE trades ADD COLUMN {mc};")
                else:
                    mc_pg = [f"ADD COLUMN IF NOT EXISTS {m}" for m in missing_cols]
                    alter_query = f"ALTER TABLE trades {', '.join(mc_pg)};"
                    cur.execute(alter_query)
                print("✅ Migration Complete.")
            else:
                print("✅ Schema is up to date.")

        cur.execute("CREATE TABLE IF NOT EXISTS bot_state (key_name VARCHAR(50) PRIMARY KEY, value_text TEXT);")
        conn.commit()

    except Exception as e:
        print(f"❌ Migration Failed: {e}")
        conn.rollback()

def get_active_signals():
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT symbol, timeframe 
            FROM trades 
            WHERE status NOT LIKE '%Closed%' 
            AND status NOT LIKE '%Cancelled%'
            AND status NOT LIKE '%Stop Loss%'
        """)
        return {(r['symbol'] if isinstance(r, dict) else r[0], r['timeframe'] if isinstance(r, dict) else r[1]) for r in cur.fetchall()}
    except Exception as e:
        print(f"⚠️ Error fetching active signals: {e}")
        return set()
    finally:
        release_conn(conn)

def get_risk_config():
    conn = get_conn()
    defaults = {
        'auto_trade': 'on',
        'total_trading_capital_usdt': '10.0',
        'max_concurrent_trades': '2',
        'max_leverage_limit': '50'
    }
    try:
        cur = conn.cursor()
        cur.execute("SELECT key_name, value_text FROM bot_state WHERE key_name IN ('auto_trade', 'total_trading_capital_usdt', 'max_concurrent_trades', 'max_leverage_limit')")
        rows = cur.fetchall()
        for row in rows:
            k = row['key_name'] if isinstance(row, dict) else row[0]
            v = row['value_text'] if isinstance(row, dict) else row[1]
            defaults[k] = v
    except: pass
    finally: release_conn(conn)
    return {
        'auto_trade': defaults.get('auto_trade', 'on') == 'on',
        'total_trading_capital_usdt': float(defaults.get('total_trading_capital_usdt', 10)),
        'max_concurrent_trades': int(defaults.get('max_concurrent_trades', 2)),
        'max_leverage_limit': int(defaults.get('max_leverage_limit', 50))
    }

def set_risk_config(key, value):
    conn = get_conn()
    try:
        cur = conn.cursor()
        if IS_SQLITE:
            cur.execute("INSERT INTO bot_state (key_name, value_text) VALUES (%s, %s) ON CONFLICT(key_name) DO UPDATE SET value_text = excluded.value_text", (key, str(value)))
        else:
            cur.execute("INSERT INTO bot_state (key_name, value_text) VALUES (%s, %s) ON CONFLICT (key_name) DO UPDATE SET value_text = EXCLUDED.value_text", (key, str(value)))
        conn.commit()
        return True
    except: return False
    finally: release_conn(conn)