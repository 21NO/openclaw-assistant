# bitcoin_trader_llm

Project skeleton for an LLM-driven Bitcoin trading system (Upbit). Designed to run via cron and have the OpenClaw agent execute pending signals.

Key points
- DRY_RUN is enabled by default. No real orders will be placed unless you explicitly disable DRY_RUN and ensure API keys are configured.
- Reflections (post-mortems) are written to the local memory directory and can be picked up by the existing LanceDB ingest pipeline for semantic search.
- The system writes signals to llm_signals and the OpenClaw agent is expected to run the executor to process pending signals.

How to start (development)
1. Create a virtualenv and install requirements:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ./scripts/install_requirements.sh
   ```
2. Prepare .env in the project root with your DB and optional OPENAI_API_KEY values. Example keys:
   - DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
   - OPENAI_API_KEY (optional)
   - DRY_RUN (true/false)
   - INDEX_REFLECTIONS (true/false)

3. Run a single cron job simulation:
   ```bash
   python3 run_cron.py
   ```

4. To process pending signals (executor) locally for testing:
   ```bash
   python3 -m app.executor
   ```

DB migrations
- See migrations/create_tables.sql. Review before running on your production DB. The statements are CREATE TABLE IF NOT EXISTS and non-destructive.

OpenClaw integration
- To have the OpenClaw agent execute pending signals, configure OpenClaw cron or agentTurn to call `python3 /root/.openclaw/workspace/projects/bitcoin_trader_llm/app/executor.py` (or use the provided Executor class via a lightweight wrapper).

Security
- Do not commit secrets to git. Keep .env with strict file permissions (chmod 600).

Notes
- This is a scaffold: production hardening, comprehensive unit tests, and thorough backtesting are recommended before enabling REAL trading.
