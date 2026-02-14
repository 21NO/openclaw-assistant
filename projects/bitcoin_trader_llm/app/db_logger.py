"""
DB logger and helper functions.
- Connects to configured DB (pymysql) if DB_HOST present, otherwise falls back to sqlite in the project directory.
- Provides functions to insert signals, mark executions, store reflections and model logs, and to create necessary tables.

Note: This module performs safe operations but will not alter existing tables except to CREATE TABLE IF NOT EXISTS. Any destructive change requires explicit approval.
"""
import logging
import os
import json
from typing import Optional, Dict, Any
from datetime import datetime, timezone

from app import config

logger = logging.getLogger('db_logger')

# lazy imports
_pymysql = None
_sqlite3 = None
try:
    import pymysql
    _pymysql = pymysql
except Exception:
    _pymysql = None

try:
    import sqlite3
    _sqlite3 = sqlite3
except Exception:
    _sqlite3 = None


# --- helpers for safe JSON serialization / normalization ---
def _normalize_for_json(obj):
    """
    Recursively convert obj into JSON-safe types:
      - datetime -> ISO8601 (UTC) string
      - numpy/pandas scalar types -> native python types
      - pandas.Timestamp/DataFrame/Series -> iso/list/dict
      - bytes -> utf-8 decoded string (fallback)
      - fallback: str(obj)
    This keeps payloads uniform before json.dumps and DB storage.
    """
    # local imports so module doesn't require numpy/pandas at import time
    try:
        import numpy as _np
    except Exception:
        _np = None
    try:
        import pandas as _pd
    except Exception:
        _pd = None

    # primitives / None
    if obj is None or isinstance(obj, (bool, int, float, str)):
        return obj

    # datetime -> ISO8601 UTC
    if isinstance(obj, datetime):
        dt = obj
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()

    # numpy scalars
    if _np is not None and isinstance(obj, _np.generic):
        try:
            return obj.item()
        except Exception:
            try:
                return float(obj)
            except Exception:
                return str(obj)

    # pandas types
    if _pd is not None:
        if isinstance(obj, _pd.Timestamp):
            ts = obj
            if ts.tzinfo is None:
                try:
                    ts = ts.tz_localize('UTC')
                except Exception:
                    pass
            try:
                return ts.isoformat()
            except Exception:
                return str(ts)
        if isinstance(obj, _pd.DataFrame):
            try:
                return obj.to_dict(orient='records')
            except Exception:
                return obj.values.tolist()
        if isinstance(obj, _pd.Series):
            try:
                return obj.tolist()
            except Exception:
                return [_normalize_for_json(v) for v in obj.values]

    # dict / list / tuple / set
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            # ensure keys are strings
            try:
                key = k if isinstance(k, str) else str(k)
            except Exception:
                key = str(k)
            out[key] = _normalize_for_json(v)
        return out
    if isinstance(obj, (list, tuple, set)):
        return [_normalize_for_json(v) for v in obj]

    # bytes
    if isinstance(obj, (bytes, bytearray)):
        try:
            return obj.decode('utf-8')
        except Exception:
            return str(obj)

    # fallback
    try:
        return str(obj)
    except Exception:
        return None


def _format_sql_datetime(dt):
    """Return a SQL DATETIME friendly string (YYYY-MM-DD HH:MM:SS) in UTC or None."""
    if dt is None:
        return None
    # numeric epoch
    if isinstance(dt, (int, float)):
        try:
            dt = datetime.fromtimestamp(float(dt), tz=timezone.utc)
        except Exception:
            return None
    # iso string
    if isinstance(dt, str):
        s = dt.strip()
        # handle trailing Z
        if s.endswith('Z'):
            s = s[:-1] + '+00:00'
        try:
            dt = datetime.fromisoformat(s)
        except Exception:
            # fallback: try common format
            try:
                dt = datetime.strptime(s, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
            except Exception:
                return None
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    return None


class DBLogger:
    def __init__(self):
        self.conn = None
        self.is_mysql = False
        self._connect()
        # ensure tables exist
        try:
            self.create_tables_if_missing()
        except Exception:
            logger.exception('create_tables_if_missing failed')

    def _connect(self):
        """Attempt to connect to MySQL when configured; otherwise use sqlite fallback.
        If MySQL connection fails, fall back to sqlite but log the error.
        """
        try:
            if config.DB_HOST and _pymysql is not None:
                try:
                    logger.info('Attempting MySQL connection (pymysql)')
                    self.conn = _pymysql.connect(host=config.DB_HOST,
                                                 port=config.DB_PORT or 3306,
                                                 user=config.DB_USER,
                                                 password=config.DB_PASSWORD,
                                                 db=config.DB_NAME,
                                                 charset='utf8mb4',
                                                 cursorclass=_pymysql.cursors.DictCursor,
                                                 autocommit=True)
                    self.is_mysql = True
                    logger.info('Connected to MySQL')
                    return
                except Exception:
                    logger.exception('MySQL connection failed; falling back to sqlite')
                    self.conn = None
                    self.is_mysql = False

            # fallback to sqlite
            dbfile = os.path.join(config.WORKSPACE, 'bitcoin_trader_llm.sqlite')
            logger.info(f'Connecting to sqlite fallback: {dbfile}')
            if _sqlite3 is None:
                raise RuntimeError('sqlite3 not available')
            self.conn = _sqlite3.connect(dbfile, check_same_thread=False)
            self.conn.row_factory = _sqlite3.Row
            self.is_mysql = False
            # improve sqlite concurrency
            try:
                cur = self.conn.cursor()
                cur.execute('PRAGMA journal_mode=WAL;')
                cur.execute('PRAGMA synchronous=NORMAL;')
                cur.close()
            except Exception:
                logger.debug('sqlite PRAGMA setup failed or unsupported')

        except Exception as e:
            logger.exception(f'DB connect failed: {e}')
            self.conn = None
            self.is_mysql = False

    def execute(self, sql: str, params: Optional[tuple] = None, retries: int = 1):
        """Execute SQL with an optional single reconnect/retry. Returns lastrowid when possible."""
        if self.conn is None:
            self._connect()
        cur = self.conn.cursor()
        try:
            if params:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
            # commit for sqlite connections so other connections see changes
            try:
                if not self.is_mysql and hasattr(self.conn, 'commit'):
                    self.conn.commit()
            except Exception:
                logger.debug('commit failed or not required')

            try:
                return cur.lastrowid
            except Exception:
                return None
        except Exception:
            logger.exception('DB execute failed')
            if retries > 0:
                logger.info('DB execute: reconnecting and retrying once')
                try:
                    self._connect()
                    return self.execute(sql, params, retries=retries-1)
                except Exception:
                    logger.exception('Retry after reconnect failed')
                    raise
            else:
                raise
        finally:
            try:
                cur.close()
            except Exception:
                pass

    def create_tables_if_missing(self):
        """
        Create the application tables if they do not exist. Non-destructive.
        Adds additional tables for cron snapshots, allocation proposals and TWAP runs.
        """
        engine = 'mysql' if self.is_mysql else 'sqlite'

        if engine == 'mysql':
            json_type = 'JSON'
            stmts = [
                """
                CREATE TABLE IF NOT EXISTS llm_signals (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    run_id VARCHAR(64),
                    strategy_name VARCHAR(100),
                    payload_json JSON,
                    suggested_pct INT,
                    confidence DOUBLE,
                    status VARCHAR(20) DEFAULT 'pending',
                    scheduled_exec_at DATETIME NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """,
                """
                CREATE TABLE IF NOT EXISTS llm_executions (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    signal_id INT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    order_id VARCHAR(200),
                    executed_at DATETIME NULL,
                    side VARCHAR(10),
                    price DOUBLE,
                    amount DOUBLE,
                    fee DOUBLE,
                    status VARCHAR(50),
                    result_json JSON
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """,
                """
                CREATE TABLE IF NOT EXISTS llm_reflections (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    execution_id INT NULL,
                    strategy_name VARCHAR(100),
                    entry_ts DATETIME NULL,
                    exit_ts DATETIME NULL,
                    pnl_krw DOUBLE,
                    pnl_pct DOUBLE,
                    duration_sec INT,
                    metrics_json JSON,
                    reflection_text TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """,
                """
                CREATE TABLE IF NOT EXISTS strategy_versions (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    strategy_name VARCHAR(100),
                    params_json JSON,
                    active TINYINT(1) DEFAULT 0,
                    reason TEXT,
                    created_by VARCHAR(100),
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """,
                """
                CREATE TABLE IF NOT EXISTS llm_decision_requests (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    run_id VARCHAR(64),
                    payload_json JSON,
                    status VARCHAR(20) DEFAULT 'awaiting',
                    assigned_to VARCHAR(100)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """,
                """
                CREATE TABLE IF NOT EXISTS model_logs (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    run_id VARCHAR(64),
                    input_json JSON,
                    output_json JSON,
                    tokens_used INT,
                    latency_ms INT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """,
                """
                CREATE TABLE IF NOT EXISTS cron_runs (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    run_id VARCHAR(128) UNIQUE,
                    started_at DATETIME,
                    finished_at DATETIME NULL,
                    total_equity_krw DOUBLE,
                    cash_krw DOUBLE,
                    reserved_krw DOUBLE,
                    summary_json JSON,
                    notes TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """,
                """
                CREATE TABLE IF NOT EXISTS cron_positions (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    run_id VARCHAR(128),
                    symbol VARCHAR(64),
                    units DOUBLE,
                    avg_price DOUBLE,
                    notional_krw DOUBLE,
                    side VARCHAR(8),
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """,
                """
                CREATE TABLE IF NOT EXISTS cron_features (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    run_id VARCHAR(128),
                    symbol VARCHAR(64),
                    feature_json JSON,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """,
                """
                CREATE TABLE IF NOT EXISTS allocation_proposals (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    run_id VARCHAR(128),
                    signal_id INT,
                    strategy_name VARCHAR(100),
                    symbol VARCHAR(64),
                    suggested_risk_pct DOUBLE,
                    stop_pct DOUBLE,
                    desired_notional_krw DOUBLE,
                    capped_notional_krw DOUBLE,
                    additional_needed_krw DOUBLE,
                    scale DOUBLE,
                    params_json JSON,
                    reason TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """,
                """
                CREATE TABLE IF NOT EXISTS twap_runs (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    run_id VARCHAR(128),
                    signal_id INT,
                    total_requested_krw DOUBLE,
                    executed_krw DOUBLE,
                    slices_planned INT,
                    slices_executed INT,
                    summary_json JSON,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """
            ]
        else:
            json_type = 'TEXT'
            stmts = [
                f"""
                CREATE TABLE IF NOT EXISTS llm_signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    run_id VARCHAR(64),
                    strategy_name VARCHAR(100),
                    payload_json {json_type},
                    suggested_pct INT,
                    confidence FLOAT,
                    status VARCHAR(20) DEFAULT 'pending',
                    scheduled_exec_at DATETIME NULL
                );
                """,
                f"""
                CREATE TABLE IF NOT EXISTS llm_executions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    signal_id INTEGER,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    order_id VARCHAR(200),
                    executed_at DATETIME NULL,
                    side VARCHAR(10),
                    price FLOAT,
                    amount FLOAT,
                    fee FLOAT,
                    status VARCHAR(50),
                    result_json {json_type}
                );
                """,
                f"""
                CREATE TABLE IF NOT EXISTS llm_reflections (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    execution_id INTEGER NULL,
                    strategy_name VARCHAR(100),
                    entry_ts DATETIME NULL,
                    exit_ts DATETIME NULL,
                    pnl_krw FLOAT,
                    pnl_pct FLOAT,
                    duration_sec INT,
                    metrics_json {json_type},
                    reflection_text TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                );
                """,
                f"""
                CREATE TABLE IF NOT EXISTS strategy_versions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    strategy_name VARCHAR(100),
                    params_json {json_type},
                    active BOOLEAN DEFAULT 0,
                    reason TEXT,
                    created_by VARCHAR(100),
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                );
                """,
                f"""
                CREATE TABLE IF NOT EXISTS llm_decision_requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    run_id VARCHAR(64),
                    payload_json {json_type},
                    status VARCHAR(20) DEFAULT 'awaiting',
                    assigned_to VARCHAR(100)
                );
                """,
                f"""
                CREATE TABLE IF NOT EXISTS model_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id VARCHAR(64),
                    input_json {json_type},
                    output_json {json_type},
                    tokens_used INT,
                    latency_ms INT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                );
                """,
                f"""
                CREATE TABLE IF NOT EXISTS cron_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id VARCHAR(128) UNIQUE,
                    started_at DATETIME,
                    finished_at DATETIME NULL,
                    total_equity_krw FLOAT,
                    cash_krw FLOAT,
                    reserved_krw FLOAT,
                    summary_json {json_type},
                    notes TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                );
                """,
                f"""
                CREATE TABLE IF NOT EXISTS cron_positions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id VARCHAR(128),
                    symbol VARCHAR(64),
                    units FLOAT,
                    avg_price FLOAT,
                    notional_krw FLOAT,
                    side VARCHAR(8),
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                );
                """,
                f"""
                CREATE TABLE IF NOT EXISTS cron_features (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id VARCHAR(128),
                    symbol VARCHAR(64),
                    feature_json {json_type},
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                );
                """,
                f"""
                CREATE TABLE IF NOT EXISTS allocation_proposals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id VARCHAR(128),
                    signal_id INTEGER,
                    strategy_name VARCHAR(100),
                    symbol VARCHAR(64),
                    suggested_risk_pct FLOAT,
                    stop_pct FLOAT,
                    desired_notional_krw FLOAT,
                    capped_notional_krw FLOAT,
                    additional_needed_krw FLOAT,
                    scale FLOAT,
                    params_json {json_type},
                    reason TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                );
                """,
                f"""
                CREATE TABLE IF NOT EXISTS twap_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id VARCHAR(128),
                    signal_id INTEGER,
                    total_requested_krw FLOAT,
                    executed_krw FLOAT,
                    slices_planned INT,
                    slices_executed INT,
                    summary_json {json_type},
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                );
                """
            ]

        for s in stmts:
            self.execute(s)

    def insert_signal(self, run_id: str, strategy_name: str, payload: Dict[str, Any], suggested_pct: int = 0, confidence: float = 0.0) -> int:
        try:
            norm = _normalize_for_json(payload)
            pj = json.dumps(norm, ensure_ascii=False, separators=(',', ':'))
            if self.is_mysql:
                sql = "INSERT INTO llm_signals (run_id,strategy_name,payload_json,suggested_pct,confidence,status) VALUES (%s,%s,%s,%s,%s,'pending')"
            else:
                sql = "INSERT INTO llm_signals (run_id,strategy_name,payload_json,suggested_pct,confidence,status) VALUES (?,?,?,?,?,'pending')"
            return self.execute(sql, (run_id, strategy_name, pj, suggested_pct, confidence))
        except Exception:
            logger.exception('insert_signal failed')
            return -1

    def insert_decision_request(self, run_id: str, payload: Dict[str, Any]) -> int:
        try:
            # normalize payload into JSON-safe primitives to avoid serialization errors
            norm = _normalize_for_json(payload)
            pj = json.dumps(norm, ensure_ascii=False, separators=(',', ':'))
            if self.is_mysql:
                sql = "INSERT INTO llm_decision_requests (run_id,payload_json,status) VALUES (%s,%s,'awaiting')"
            else:
                sql = "INSERT INTO llm_decision_requests (run_id,payload_json,status) VALUES (?,?, 'awaiting')"
            return self.execute(sql, (run_id, pj))
        except Exception:
            logger.exception('insert_decision_request failed')
            return -1

    def get_pending_decision_requests(self, limit: int = 20):
        try:
            cur = self.conn.cursor()
            if self.is_mysql:
                cur.execute("SELECT * FROM llm_decision_requests WHERE status='awaiting' ORDER BY created_at ASC LIMIT %s", (limit,))
            else:
                cur.execute("SELECT * FROM llm_decision_requests WHERE status='awaiting' ORDER BY created_at ASC LIMIT ?", (limit,))
            rows = cur.fetchall()
            out = []
            for r in rows:
                if hasattr(r, 'items'):
                    d = dict(r.items())
                else:
                    d = dict(r)
                out.append(d)
            return out
        except Exception:
            logger.exception('get_pending_decision_requests failed')
            return []

    def update_decision_request_status(self, request_id: int, status: str, assigned_to: Optional[str] = None):
        try:
            if assigned_to is None:
                if self.is_mysql:
                    sql = "UPDATE llm_decision_requests SET status=%s WHERE id=%s"
                else:
                    sql = "UPDATE llm_decision_requests SET status=? WHERE id=?"
                self.execute(sql, (status, request_id))
            else:
                if self.is_mysql:
                    sql = "UPDATE llm_decision_requests SET status=%s, assigned_to=%s WHERE id=%s"
                else:
                    sql = "UPDATE llm_decision_requests SET status=?, assigned_to=? WHERE id=?"
                self.execute(sql, (status, assigned_to, request_id))
        except Exception:
            logger.exception('update_decision_request_status failed')

    def get_pending_signals(self, limit: int = 20):
        try:
            cur = self.conn.cursor()
            if self.is_mysql:
                cur.execute("SELECT * FROM llm_signals WHERE status='pending' ORDER BY created_at ASC LIMIT %s", (limit,))
            else:
                cur.execute("SELECT * FROM llm_signals WHERE status='pending' ORDER BY created_at ASC LIMIT ?", (limit,))
            rows = cur.fetchall()
            # normalize rows to dicts
            out = []
            for r in rows:
                if hasattr(r, 'items'):
                    d = dict(r.items())
                else:
                    # sqlite Row
                    d = dict(r)
                out.append(d)
            return out
        except Exception:
            logger.exception('get_pending_signals failed')
            return []

    def update_signal_status(self, signal_id: int, status: str):
        try:
            if self.is_mysql:
                sql = "UPDATE llm_signals SET status=%s WHERE id=%s"
            else:
                sql = "UPDATE llm_signals SET status=? WHERE id=?"
            self.execute(sql, (status, signal_id))
        except Exception:
            logger.exception('update_signal_status failed')

    def insert_execution(self, signal_id: int, side: str, price: float, amount: float, fee: float, status: str, result: Dict[str, Any]) -> int:
        try:
            # normalize result and prepare executed_at for SQL
            norm_result = _normalize_for_json(result) if isinstance(result, dict) else _normalize_for_json(result)
            order_id = norm_result.get('order_id') if isinstance(norm_result, dict) else None
            executed_at_raw = norm_result.get('executed_at') if isinstance(norm_result, dict) else None
            executed_at_sql = _format_sql_datetime(executed_at_raw)
            resj = json.dumps(norm_result, ensure_ascii=False, separators=(',', ':'))
            if self.is_mysql:
                sql = "INSERT INTO llm_executions (signal_id,order_id,executed_at,side,price,amount,fee,status,result_json) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            else:
                sql = "INSERT INTO llm_executions (signal_id,order_id,executed_at,side,price,amount,fee,status,result_json) VALUES (?,?,?,?,?,?,?,?,?)"
            return self.execute(sql, (signal_id, order_id, executed_at_sql, side, price, amount, fee, status, resj))
        except Exception:
            logger.exception('insert_execution failed')
            return -1

    def insert_reflection(self, execution_id: Optional[int], strategy_name: str, entry_ts, exit_ts, pnl_krw: float, pnl_pct: float, duration_sec: int, metrics: Dict[str, Any], text: str):
        try:
            entry_sql = _format_sql_datetime(entry_ts)
            exit_sql = _format_sql_datetime(exit_ts)
            metrics_json = json.dumps(_normalize_for_json(metrics), ensure_ascii=False, separators=(',', ':'))
            if self.is_mysql:
                sql = "INSERT INTO llm_reflections (execution_id,strategy_name,entry_ts,exit_ts,pnl_krw,pnl_pct,duration_sec,metrics_json,reflection_text) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            else:
                sql = "INSERT INTO llm_reflections (execution_id,strategy_name,entry_ts,exit_ts,pnl_krw,pnl_pct,duration_sec,metrics_json,reflection_text) VALUES (?,?,?,?,?,?,?,?,?)"
            return self.execute(sql, (execution_id, strategy_name, entry_sql, exit_sql, pnl_krw, pnl_pct, duration_sec, metrics_json, text))
        except Exception:
            logger.exception('insert_reflection_failed')
            return -1

    def record_strategy_version(self, strategy_name: str, params: Dict[str, Any], reason: str = ''):
        try:
            params_json = json.dumps(_normalize_for_json(params), ensure_ascii=False, separators=(',', ':'))
            if self.is_mysql:
                sql = "INSERT INTO strategy_versions (strategy_name,params_json,active,reason,created_by) VALUES (%s,%s,%s,%s,%s)"
            else:
                sql = "INSERT INTO strategy_versions (strategy_name,params_json,active,reason,created_by) VALUES (?,?,?,?,?)"
            return self.execute(sql, (strategy_name, params_json, 1, reason, 'auto'))
        except Exception:
            logger.exception('record_strategy_version failed')
            return -1

    def get_active_strategy(self):
        try:
            cur = self.conn.cursor()
            if self.is_mysql:
                cur.execute("SELECT strategy_name, params_json FROM strategy_versions WHERE active=1 ORDER BY created_at DESC LIMIT 1")
            else:
                cur.execute("SELECT strategy_name, params_json FROM strategy_versions WHERE active=1 ORDER BY created_at DESC LIMIT 1")
            row = cur.fetchone()
            if not row:
                return None
            if hasattr(row, 'keys'):
                return {'name': row['strategy_name'], 'params': json.loads(row['params_json'])}
            else:
                # sqlite Row
                return {'name': row[0], 'params': json.loads(row[1])}
        except Exception:
            # no active strategy saved
            return None

    # --- cron / allocation helpers ---
    def insert_cron_run(self, run_id: str, started_at, finished_at=None, total_equity_krw: float = 0.0, cash_krw: float = 0.0, reserved_krw: float = 0.0, summary: Optional[Dict[str, Any]] = None, notes: str = '') -> int:
        try:
            started = _format_sql_datetime(started_at)
            finished = _format_sql_datetime(finished_at)
            summary_json = json.dumps(_normalize_for_json(summary), ensure_ascii=False, separators=(',', ':')) if summary is not None else None
            if self.is_mysql:
                sql = "INSERT INTO cron_runs (run_id,started_at,finished_at,total_equity_krw,cash_krw,reserved_krw,summary_json,notes) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
                params = (run_id, started, finished, total_equity_krw, cash_krw, reserved_krw, summary_json, notes)
            else:
                sql = "INSERT INTO cron_runs (run_id,started_at,finished_at,total_equity_krw,cash_krw,reserved_krw,summary_json,notes) VALUES (?,?,?,?,?,?,?,?)"
                params = (run_id, started, finished, total_equity_krw, cash_krw, reserved_krw, summary_json, notes)
            return self.execute(sql, params)
        except Exception:
            logger.exception('insert_cron_run failed')
            return -1

    def insert_cron_position(self, run_id: str, symbol: str, units: float, avg_price: float, notional_krw: float, side: str = 'long') -> int:
        try:
            if self.is_mysql:
                sql = "INSERT INTO cron_positions (run_id,symbol,units,avg_price,notional_krw,side) VALUES (%s,%s,%s,%s,%s,%s)"
            else:
                sql = "INSERT INTO cron_positions (run_id,symbol,units,avg_price,notional_krw,side) VALUES (?,?,?,?,?,?)"
            return self.execute(sql, (run_id, symbol, units, avg_price, notional_krw, side))
        except Exception:
            logger.exception('insert_cron_position failed')
            return -1

    def insert_cron_feature(self, run_id: str, symbol: str, feature: Dict[str, Any]) -> int:
        try:
            fj = json.dumps(_normalize_for_json(feature), ensure_ascii=False, separators=(',', ':'))
            if self.is_mysql:
                sql = "INSERT INTO cron_features (run_id,symbol,feature_json) VALUES (%s,%s,%s)"
            else:
                sql = "INSERT INTO cron_features (run_id,symbol,feature_json) VALUES (?,?,?)"
            return self.execute(sql, (run_id, symbol, fj))
        except Exception:
            logger.exception('insert_cron_feature failed')
            return -1

    def insert_allocation_proposal(self, run_id: str, signal_id: int, strategy_name: str, symbol: str, suggested_risk_pct: float, stop_pct: float, desired_notional_krw: float, capped_notional_krw: float, additional_needed_krw: float, scale: float, params: Dict[str, Any], reason: str = '') -> int:
        try:
            pjson = json.dumps(_normalize_for_json(params), ensure_ascii=False, separators=(',', ':'))
            if self.is_mysql:
                sql = "INSERT INTO allocation_proposals (run_id,signal_id,strategy_name,symbol,suggested_risk_pct,stop_pct,desired_notional_krw,capped_notional_krw,additional_needed_krw,scale,params_json,reason) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            else:
                sql = "INSERT INTO allocation_proposals (run_id,signal_id,strategy_name,symbol,suggested_risk_pct,stop_pct,desired_notional_krw,capped_notional_krw,additional_needed_krw,scale,params_json,reason) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"
            return self.execute(sql, (run_id, signal_id, strategy_name, symbol, suggested_risk_pct, stop_pct, desired_notional_krw, capped_notional_krw, additional_needed_krw, scale, pjson, reason))
        except Exception:
            logger.exception('insert_allocation_proposal failed')
            return -1

    def insert_twap_run(self, run_id: str, signal_id: int, total_requested_krw: float, executed_krw: float, slices_planned: int, slices_executed: int, summary: Dict[str, Any]) -> int:
        try:
            sj = json.dumps(_normalize_for_json(summary), ensure_ascii=False, separators=(',', ':'))
            if self.is_mysql:
                sql = "INSERT INTO twap_runs (run_id,signal_id,total_requested_krw,executed_krw,slices_planned,slices_executed,summary_json) VALUES (%s,%s,%s,%s,%s,%s,%s)"
            else:
                sql = "INSERT INTO twap_runs (run_id,signal_id,total_requested_krw,executed_krw,slices_planned,slices_executed,summary_json) VALUES (?,?,?,?,?,?,?)"
            return self.execute(sql, (run_id, signal_id, total_requested_krw, executed_krw, slices_planned, slices_executed, sj))
        except Exception:
            logger.exception('insert_twap_run failed')
            return -1

    def get_invested(self, run_id: str) -> float:
        try:
            cur = self.conn.cursor()
            if self.is_mysql:
                cur.execute("SELECT COALESCE(SUM(notional_krw),0) as invested FROM cron_positions WHERE run_id=%s", (run_id,))
            else:
                cur.execute("SELECT COALESCE(SUM(notional_krw),0) as invested FROM cron_positions WHERE run_id=?", (run_id,))
            row = cur.fetchone()
            if not row:
                return 0.0
            if hasattr(row, 'keys'):
                return float(row['invested'] or 0.0)
            else:
                return float(row[0] or 0.0)
        except Exception:
            logger.exception('get_invested failed')
            return 0.0

    def get_symbol_exposure(self, run_id: str, symbol: str) -> float:
        try:
            cur = self.conn.cursor()
            if self.is_mysql:
                cur.execute("SELECT COALESCE(SUM(notional_krw),0) as exposure FROM cron_positions WHERE run_id=%s AND symbol=%s", (run_id, symbol))
            else:
                cur.execute("SELECT COALESCE(SUM(notional_krw),0) as exposure FROM cron_positions WHERE run_id=? AND symbol=?", (run_id, symbol))
            row = cur.fetchone()
            if not row:
                return 0.0
            if hasattr(row, 'keys'):
                return float(row['exposure'] or 0.0)
            else:
                return float(row[0] or 0.0)
        except Exception:
            logger.exception('get_symbol_exposure failed')
            return 0.0

    def get_cron_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        try:
            cur = self.conn.cursor()
            if self.is_mysql:
                cur.execute("SELECT * FROM cron_runs WHERE run_id=%s LIMIT 1", (run_id,))
            else:
                cur.execute("SELECT * FROM cron_runs WHERE run_id=? LIMIT 1", (run_id,))
            row = cur.fetchone()
            if not row:
                return None
            if hasattr(row, 'keys'):
                d = dict(row)
            else:
                d = dict(row)
            # try to parse summary_json if present
            if 'summary_json' in d and d['summary_json']:
                try:
                    d['summary'] = json.loads(d['summary_json'])
                except Exception:
                    d['summary'] = d['summary_json']
            return d
        except Exception:
            logger.exception('get_cron_run failed')
            return None

    def get_recent_reflections(self, limit: int = 10):
        try:
            cur = self.conn.cursor()
            if self.is_mysql:
                cur.execute("SELECT * FROM llm_reflections ORDER BY created_at DESC LIMIT %s", (limit,))
            else:
                cur.execute("SELECT * FROM llm_reflections ORDER BY created_at DESC LIMIT ?", (limit,))
            rows = cur.fetchall()
            out = []
            for r in rows:
                if hasattr(r, 'items'):
                    d = dict(r.items())
                else:
                    d = dict(r)
                # parse metrics_json
                if 'metrics_json' in d and d['metrics_json']:
                    try:
                        d['metrics'] = json.loads(d['metrics_json'])
                    except Exception:
                        d['metrics'] = d['metrics_json']
                out.append(d)
            return out
        except Exception:
            logger.exception('get_recent_reflections failed')
            return []
