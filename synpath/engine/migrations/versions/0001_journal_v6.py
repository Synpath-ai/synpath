"""Freeze the pre-Alembic schema; adopt compatible preexisting journals."""
from alembic import op
revision = "0001_journal_v6"
down_revision = None
branch_labels = None
depends_on = None
SCHEMA = """
CREATE TABLE IF NOT EXISTS submissions (
    client_order_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    account_key TEXT NOT NULL,
    venue TEXT NOT NULL,
    request TEXT NOT NULL,
    effective_request TEXT NOT NULL,
    result_id TEXT,
    state TEXT NOT NULL DEFAULT 'pending',
    error TEXT,
    created_ts INTEGER NOT NULL,
    updated_ts INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS submissions_owner ON submissions(user_id, account_key, created_ts);

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    kind TEXT NOT NULL,
    key TEXT,
    payload TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS events_kind ON events(kind, seq);
CREATE INDEX IF NOT EXISTS events_key ON events(key, seq);

CREATE TABLE IF NOT EXISTS intents (
    client_order_id TEXT PRIMARY KEY,
    state TEXT NOT NULL,
    operation TEXT NOT NULL,
    venue TEXT NOT NULL,
    account_key TEXT NOT NULL,
    market_id TEXT,
    order_id TEXT,
    target_order_id TEXT,
    book TEXT,
    trader TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    request TEXT NOT NULL,
    detail TEXT,
    created_ts INTEGER NOT NULL,
    updated_ts INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS intents_state ON intents(state, updated_ts);
CREATE INDEX IF NOT EXISTS intents_order ON intents(venue, order_id);

CREATE TABLE IF NOT EXISTS orders (
    venue TEXT NOT NULL,
    id TEXT NOT NULL,
    client_order_id TEXT,
    account_key TEXT NOT NULL,
    market_id TEXT NOT NULL,
    side TEXT NOT NULL,
    status TEXT NOT NULL,
    terminal INTEGER NOT NULL DEFAULT 0,
    book TEXT,
    trader TEXT,
    quantity TEXT NOT NULL,
    filled TEXT NOT NULL,
    price TEXT,
    parent_id TEXT,
    payload TEXT NOT NULL,
    created_ts INTEGER,
    updated_ts INTEGER NOT NULL,
    PRIMARY KEY (venue, id)
);
CREATE INDEX IF NOT EXISTS orders_open ON orders(terminal, venue, account_key);
CREATE INDEX IF NOT EXISTS orders_client ON orders(client_order_id);

CREATE TABLE IF NOT EXISTS fills (
    venue TEXT NOT NULL,
    id TEXT NOT NULL,
    order_id TEXT NOT NULL,
    client_order_id TEXT,
    account_key TEXT NOT NULL,
    market_id TEXT NOT NULL,
    side TEXT NOT NULL,
    price TEXT NOT NULL,
    quantity TEXT NOT NULL,
    fee TEXT,
    settlement TEXT NOT NULL,
    book TEXT,
    trader TEXT,
    ts INTEGER NOT NULL,
    payload TEXT NOT NULL,
    PRIMARY KEY (venue, id)
);
CREATE INDEX IF NOT EXISTS fills_order ON fills(venue, order_id);
CREATE INDEX IF NOT EXISTS fills_ts ON fills(ts);

CREATE TABLE IF NOT EXISTS managed_orders (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    state TEXT NOT NULL,
    account_key TEXT,
    market_id TEXT NOT NULL,
    book TEXT,
    trader TEXT,
    payload TEXT NOT NULL,
    child_order_id TEXT,
    created_ts INTEGER NOT NULL,
    updated_ts INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS managed_state ON managed_orders(state);

CREATE TABLE IF NOT EXISTS leases (
    name TEXT PRIMARY KEY,
    owner TEXT NOT NULL,
    holder TEXT NOT NULL,
    acquired_ts INTEGER NOT NULL,
    expires_ts INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS config_versions (
    version INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,
    ts INTEGER NOT NULL,
    author TEXT,
    payload TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS fair_values (
    account_key TEXT NOT NULL,
    market_id TEXT NOT NULL,
    value TEXT NOT NULL,
    source TEXT,
    ts INTEGER NOT NULL,
    PRIMARY KEY (account_key, market_id)
);

CREATE TABLE IF NOT EXISTS buckets (
    id TEXT PRIMARY KEY,
    book TEXT NOT NULL,
    name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    created_ts INTEGER NOT NULL,
    updated_ts INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS bucket_members (
    bucket_id TEXT NOT NULL REFERENCES buckets(id) ON DELETE CASCADE,
    market_id TEXT NOT NULL,
    flip INTEGER NOT NULL DEFAULT 0,
    position INTEGER NOT NULL,
    PRIMARY KEY (bucket_id, market_id)
);

CREATE TABLE IF NOT EXISTS cursors (
    name TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_ts INTEGER NOT NULL
);

            CREATE TABLE IF NOT EXISTS market_orders (
                venue TEXT NOT NULL, id TEXT NOT NULL,
                user_id TEXT NOT NULL CHECK(length(trim(user_id)) > 0), account_key TEXT NOT NULL,
                client_order_id TEXT, parent_id TEXT,
                market_id TEXT NOT NULL, side TEXT NOT NULL,
                quantity TEXT NOT NULL, filled TEXT NOT NULL DEFAULT '0',
                remaining TEXT, state TEXT NOT NULL,
                time_in_force TEXT NOT NULL, expires_at INTEGER,
                reduce_only INTEGER NOT NULL DEFAULT 0,
                price TEXT,
                request TEXT, snapshot TEXT NOT NULL,
                payload_version INTEGER NOT NULL DEFAULT 1,
                venue_order_id TEXT, created_ts INTEGER NOT NULL,
                updated_ts INTEGER NOT NULL,
                PRIMARY KEY (venue, id)
            );
            CREATE UNIQUE INDEX IF NOT EXISTS market_orders_client
                ON market_orders(user_id, account_key, client_order_id)
                WHERE client_order_id IS NOT NULL;
            CREATE INDEX IF NOT EXISTS market_orders_state
                ON market_orders(user_id, state, updated_ts);


            CREATE TABLE IF NOT EXISTS limit_orders (
                venue TEXT NOT NULL, id TEXT NOT NULL,
                user_id TEXT NOT NULL CHECK(length(trim(user_id)) > 0), account_key TEXT NOT NULL,
                client_order_id TEXT, parent_id TEXT,
                market_id TEXT NOT NULL, side TEXT NOT NULL,
                quantity TEXT NOT NULL, filled TEXT NOT NULL DEFAULT '0',
                remaining TEXT, state TEXT NOT NULL,
                time_in_force TEXT NOT NULL, expires_at INTEGER,
                reduce_only INTEGER NOT NULL DEFAULT 0,
                price TEXT CHECK (request IS NULL OR price IS NOT NULL),
                request TEXT, snapshot TEXT NOT NULL,
                payload_version INTEGER NOT NULL DEFAULT 1,
                venue_order_id TEXT, created_ts INTEGER NOT NULL,
                updated_ts INTEGER NOT NULL,
                PRIMARY KEY (venue, id)
            );
            CREATE UNIQUE INDEX IF NOT EXISTS limit_orders_client
                ON limit_orders(user_id, account_key, client_order_id)
                WHERE client_order_id IS NOT NULL;
            CREATE INDEX IF NOT EXISTS limit_orders_state
                ON limit_orders(user_id, state, updated_ts);


            CREATE TABLE IF NOT EXISTS stop_market_orders (
                venue TEXT NOT NULL, id TEXT NOT NULL,
                user_id TEXT NOT NULL CHECK(length(trim(user_id)) > 0), account_key TEXT NOT NULL,
                client_order_id TEXT, parent_id TEXT,
                market_id TEXT NOT NULL, side TEXT NOT NULL,
                quantity TEXT NOT NULL, filled TEXT NOT NULL DEFAULT '0',
                remaining TEXT, state TEXT NOT NULL,
                time_in_force TEXT NOT NULL, expires_at INTEGER,
                reduce_only INTEGER NOT NULL DEFAULT 0,
                stop_price TEXT CHECK (request IS NULL OR stop_price IS NOT NULL), trigger_source TEXT CHECK (request IS NULL OR trigger_source IS NOT NULL),
                request TEXT, snapshot TEXT NOT NULL,
                payload_version INTEGER NOT NULL DEFAULT 1,
                venue_order_id TEXT, created_ts INTEGER NOT NULL,
                updated_ts INTEGER NOT NULL,
                PRIMARY KEY (venue, id)
            );
            CREATE UNIQUE INDEX IF NOT EXISTS stop_market_orders_client
                ON stop_market_orders(user_id, account_key, client_order_id)
                WHERE client_order_id IS NOT NULL;
            CREATE INDEX IF NOT EXISTS stop_market_orders_state
                ON stop_market_orders(user_id, state, updated_ts);


            CREATE TABLE IF NOT EXISTS stop_limit_orders (
                venue TEXT NOT NULL, id TEXT NOT NULL,
                user_id TEXT NOT NULL CHECK(length(trim(user_id)) > 0), account_key TEXT NOT NULL,
                client_order_id TEXT, parent_id TEXT,
                market_id TEXT NOT NULL, side TEXT NOT NULL,
                quantity TEXT NOT NULL, filled TEXT NOT NULL DEFAULT '0',
                remaining TEXT, state TEXT NOT NULL,
                time_in_force TEXT NOT NULL, expires_at INTEGER,
                reduce_only INTEGER NOT NULL DEFAULT 0,
                price TEXT CHECK (request IS NULL OR price IS NOT NULL), stop_price TEXT CHECK (request IS NULL OR stop_price IS NOT NULL), trigger_source TEXT CHECK (request IS NULL OR trigger_source IS NOT NULL),
                request TEXT, snapshot TEXT NOT NULL,
                payload_version INTEGER NOT NULL DEFAULT 1,
                venue_order_id TEXT, created_ts INTEGER NOT NULL,
                updated_ts INTEGER NOT NULL,
                PRIMARY KEY (venue, id)
            );
            CREATE UNIQUE INDEX IF NOT EXISTS stop_limit_orders_client
                ON stop_limit_orders(user_id, account_key, client_order_id)
                WHERE client_order_id IS NOT NULL;
            CREATE INDEX IF NOT EXISTS stop_limit_orders_state
                ON stop_limit_orders(user_id, state, updated_ts);
        """

def upgrade():
    source = SCHEMA
    if op.get_bind().dialect.name == "postgresql":
        import re
        source = source.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "BIGSERIAL PRIMARY KEY")
        source = re.sub(r"\bINTEGER\b", "BIGINT", source)
    for statement in source.split(";"):
        if statement.strip():
            op.execute(statement)

def downgrade():
    raise RuntimeError("baseline downgrade is destructive; restore a verified backup")
