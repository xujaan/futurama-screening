import psycopg2
from psycopg2 import pool
from modules.config_loader import CONFIG

DB_POOL = None

def init_db():
    global DB_POOL
    try:
        # Create Connection Pool
        pool_size = CONFIG['system']['max_threads'] + 5
        DB_POOL = psycopg2.pool.ThreadedConnectionPool(
            minconn=1, maxconn=pool_size,
            host=CONFIG['database']['host'], 
            database=CONFIG['database']['database'],
            user=CONFIG['database']['user'], 
            password=CONFIG['database']['password'],
            port=CONFIG['database']['port']
        )
        
        # Run Migration Logic on Startup
        conn = DB_POOL.getconn()
        try:
            migrate_schema(conn)
        finally:
            DB_POOL.putconn(conn)
            
        print("✅ Database Connected & Migrated.")
        
    except Exception as e:
        print(f"❌ DB Init Error: {e}")
        exit(1)
        
def migrate_schema(conn):
    """
    Checks the existing 'trades' table and adds missing columns dynamically.
    Renames table if strictly incompatible (fallback).
    """
    cur = conn.cursor()
    
    # 1. Define the Desired Schema
    # Format: {column_name: data_type}
    required_columns = {
        "id": "SERIAL PRIMARY KEY",
        "symbol": "VARCHAR(100)", 
        "side": "VARCHAR(10)", 
        "timeframe": "VARCHAR(5)", 
        "pattern": "VARCHAR(50)",
        "entry_price": "DECIMAL", 
        "sl_price": "DECIMAL", 
        "tp1": "DECIMAL", 
        "tp2": "DECIMAL", 
        "tp3": "DECIMAL",
        "rr": "DECIMAL",
        "status": "VARCHAR(50) DEFAULT 'Waiting Entry'", 
        "reason": "TEXT",
        "tech_score": "INT", 
        "quant_score": "INT", 
        "deriv_score": "INT", 
        "smc_score": "INT DEFAULT 0",      # NEW
        "z_score": "DECIMAL DEFAULT 0",    # NEW
        "zeta_score": "DECIMAL DEFAULT 0", # NEW
        "obi": "DECIMAL DEFAULT 0",        # NEW
        "basis": "DECIMAL", 
        "btc_bias": "VARCHAR(50)",
        "tech_reasons": "TEXT",            # NEW
        "quant_reasons": "TEXT",           # NEW
        "deriv_reasons": "TEXT",           # NEW
        "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP", 
        "entry_hit_at": "TIMESTAMP", 
        "closed_at": "TIMESTAMP", 
        "exit_price": "DECIMAL", 
        "message_id": "VARCHAR(50)", 
        "channel_id": "VARCHAR(50)"
    }

    try:
        # 2. Check if table exists
        cur.execute("SELECT to_regclass('public.trades');")
        if cur.fetchone()[0] is None:
            # Table does not exist, create it fresh
            print("🆕 Creating 'trades' table...")
            create_query = "CREATE TABLE trades (" + ", ".join([f"{k} {v}" for k, v in required_columns.items()]) + ");"
            cur.execute(create_query)
        else:
            # 3. Table exists: Check for missing columns
            print("🔍 Checking schema integrity...")
            cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'trades';")
            existing_cols = {row[0] for row in cur.fetchall()}
            
            missing_cols = []
            for col, dtype in required_columns.items():
                if col not in existing_cols:
                    # Clean up SERIAL/PRIMARY KEY for ALTER statements
                    clean_type = dtype.replace("SERIAL PRIMARY KEY", "INT").replace("PRIMARY KEY", "")
                    missing_cols.append(f"ADD COLUMN IF NOT EXISTS {col} {clean_type}")
            
            if missing_cols:
                print(f"🛠️ Migrating: Adding {len(missing_cols)} new columns...")
                alter_query = f"ALTER TABLE trades {', '.join(missing_cols)};"
                cur.execute(alter_query)
                print("✅ Migration Complete: Columns added.")
            else:
                print("✅ Schema is up to date.")

        # 4. Create Bot State Table (for Dashboard ID)
        cur.execute("CREATE TABLE IF NOT EXISTS bot_state (key_name VARCHAR(50) PRIMARY KEY, value_text TEXT);")
        
        conn.commit()

    except Exception as e:
        print(f"❌ Migration Failed: {e}")
        conn.rollback()
        # Optional: Rename broken table logic could go here if ALTER fails
        raise e

def get_conn():
    if not DB_POOL: init_db()
    return DB_POOL.getconn()

def release_conn(conn):
    if DB_POOL: DB_POOL.putconn(conn)