-- SQL migration: create application tables for bitcoin_trader_llm
-- This file is a safe, non-destructive set of CREATE TABLE IF NOT EXISTS statements.

CREATE TABLE IF NOT EXISTS llm_signals (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  run_id VARCHAR(64),
  strategy_name VARCHAR(100),
  payload_json JSON,
  suggested_pct INT,
  confidence FLOAT,
  status VARCHAR(20) DEFAULT 'pending',
  scheduled_exec_at DATETIME NULL
);

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
  result_json JSON
);

CREATE TABLE IF NOT EXISTS llm_reflections (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  execution_id INTEGER NULL,
  strategy_name VARCHAR(100),
  entry_ts DATETIME NULL,
  exit_ts DATETIME NULL,
  pnl_krw FLOAT,
  pnl_pct FLOAT,
  duration_sec INT,
  metrics_json JSON,
  reflection_text TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS strategy_versions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  strategy_name VARCHAR(100),
  params_json JSON,
  active BOOLEAN DEFAULT 0,
  reason TEXT,
  created_by VARCHAR(100),
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS llm_decision_requests (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  run_id VARCHAR(64),
  payload_json JSON,
  status VARCHAR(20) DEFAULT 'awaiting',
  assigned_to VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS model_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id VARCHAR(64),
  input_json JSON,
  output_json JSON,
  tokens_used INT,
  latency_ms INT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
