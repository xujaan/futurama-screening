import psycopg2
from psycopg2 import pool
from modules.config_loader import CONFIG

DB_POOL = None

def init_db():
    global DB_POOL
    try:
        pool_size = CONFIG['system']['max_threads'] + 5
        DB_POOL = psycopg2.pool.ThreadedConnectionPool(
            minconn=1, maxconn=pool_size,
            host=CONFIG['database']['host'], 
            database=CONFIG['database']['database'],
            user=CONFIG['database']['user'], 
            password=CONFIG['database']['password'],
            port=CONFIG['database']['port']
        )
        
        conn = DB_POOL.getconn()
        try:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(100), side VARCHAR(10), timeframe VARCHAR(5), pattern VARCHAR(50),
                    entry_price DECIMAL, sl_price DECIMAL, tp1 DECIMAL, tp2 DECIMAL, tp3 DECIMAL,
                    status VARCHAR(50) DEFAULT 'Waiting Entry', reason TEXT,
                    tech_score INT, quant_score INT, deriv_score INT,
                    basis DECIMAL, btc_bias VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
                    entry_hit_at TIMESTAMP, closed_at TIMESTAMP, exit_price DECIMAL, 
                    message_id VARCHAR(50), channel_id VARCHAR(50)
                );
            """)
            cur.execute("CREATE TABLE IF NOT EXISTS bot_state (key_name VARCHAR(50) PRIMARY KEY, value_text TEXT);")
            conn.commit()
        finally:
            DB_POOL.putconn(conn)
    except Exception as e:
        print(f"❌ DB Error: {e}")
        exit(1)

def get_conn():
    if not DB_POOL: init_db()
    return DB_POOL.getconn()

def release_conn(conn):
    if DB_POOL: DB_POOL.putconn(conn)