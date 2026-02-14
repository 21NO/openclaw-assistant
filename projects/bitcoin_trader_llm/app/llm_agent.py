"""
LLM agent interface.
- decide(...) sends a structured prompt to the configured LLM and returns a structured JSON-like dict
- generate_reflection(...) creates a human-readable post-mortem using the model (or a template fallback)

Support modes:
 - openai: uses openai.ChatCompletion (if OPENAI_API_KEY provided)
 - local endpoint: POST to LLM_ENDPOINT (config.LLM_ENDPOINT) expecting JSON reply
 - mock: deterministic fallback for testing

This module is intentionally defensive — missing libs or keys will not raise at import time.
"""
import logging
import json
import os
from typing import Dict, Any, List

from app import config

logger = logging.getLogger('llm_agent')

# lazy import openai if needed
_openai = None
try:
    if config.LLM_MODE == 'openai' and config.LLM_OPENAI_API_KEY:
        import openai as _openai
        _openai.api_key = config.LLM_OPENAI_API_KEY
except Exception:
    _openai = None

import requests


class LLMAgent:
    def __init__(self, mode: str = None):
        self.mode = mode or config.LLM_MODE
        self.endpoint = config.LLM_ENDPOINT

    def decide(self, run_id: str, symbol: str, features: dict, recent_ohlcv: List[dict], news: List[dict], balances: dict, strategy: dict) -> Dict[str, Any]:
        """
        Build a prompt and request a decision from the model.
        Returns a dict: { action: 'buy'|'sell'|'hold', pct: int, confidence: float, stop_rel: float|null, reason: str }
        """
        prompt = self._build_decision_prompt(symbol, features, recent_ohlcv, news, balances, strategy)

        try:
            if self.mode == 'openai' and _openai is not None:
                # Chat completions API (pseudo)
                logger.debug('Calling OpenAI for decision')
                response = _openai.ChatCompletion.create(
                    model=os.getenv('OPENAI_MODEL', 'gpt-4o-mini'),
                    messages=[
                        {"role": "system", "content": "You are a concise trading decision assistant. Reply EXACTLY with JSON only."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.0,
                    max_tokens=300
                )
                text = response['choices'][0]['message']['content']
                return self._parse_decision_text(text)

            elif self.mode == 'local':
                logger.debug(f'POST to local LLM endpoint: {self.endpoint}')
                r = requests.post(self.endpoint, json={"prompt": prompt}, timeout=config.REQUEST_TIMEOUT)
                r.raise_for_status()
                data = r.json()
                # expect {'decision': {...}} or raw JSON
                return data.get('decision') if isinstance(data, dict) and 'decision' in data else data

            else:
                # mock fallback: hold
                logger.debug('LLM in mock mode - returning HOLD')
                return {
                    'action': 'hold',
                    'pct': 0,
                    'confidence': 0.0,
                    'stop_rel': None,
                    'reason': 'mock fallback - no model configured'
                }
        except Exception as e:
            logger.exception(f'LLM call failed: {e}')
            # safe fallback
            return {
                'action': 'hold',
                'pct': 0,
                'confidence': 0.0,
                'stop_rel': None,
                'reason': f'error: {e}'
            }

    def generate_reflection(self, execution_record: dict, metrics: dict, recent_trades: List[dict]) -> str:
        """
        Create a human-readable reflection/post-mortem using the model if available,
        otherwise produce a template-based summary.
        """
        template = (
            f"Execution summary:\n- strategy: {execution_record.get('strategy_name')}\n"
            f"- entry: {execution_record.get('entry_price')} at {execution_record.get('executed_at')}\n"
            f"- exit: {execution_record.get('exit_price')}\n"
            f"- pnl_krw: {metrics.get('pnl_krw')}\n"
            f"- pnl_pct: {metrics.get('pnl_pct')}\n"
            "\nSuggested improvements:\n1. Check slippage and execution latency.\n2. Revisit stop sizing.\n"
        )

        # try to ask the model for a polished reflection
        try:
            if self.mode == 'openai' and _openai is not None:
                logger.debug('Calling OpenAI to generate reflection text')
                prompt = (
                    f"Given execution record: {json.dumps(execution_record)}\nmetrics: {json.dumps(metrics)}\nrecent_trades: {json.dumps(recent_trades[:10])}\n\nWrite a concise post-mortem with reasons why the trade succeeded or failed and 3 suggested improvements."
                )
                response = _openai.ChatCompletion.create(
                    model=os.getenv('OPENAI_MODEL', 'gpt-4o-mini'),
                    messages=[
                        {"role":"system","content":"You are an experienced quantitative trader summarizer."},
                        {"role":"user","content":prompt}
                    ],
                    temperature=0.2,
                    max_tokens=400
                )
                text = response['choices'][0]['message']['content']
                return text
        except Exception:
            logger.exception('Reflection generation via OpenAI failed; falling back to template')

        return template

    def _build_decision_prompt(self, symbol, features, recent_ohlcv, news, balances, strategy) -> str:
        # Build a compact, structured prompt. Keep it concise to reduce tokens.
        p = {
            'symbol': symbol,
            'features': features,
            'strategy': {k: strategy.get(k) for k in ('name', 'params') if strategy},
            'recent_ohlcv_head': recent_ohlcv[-12:] if recent_ohlcv else [],
            'news': [n.get('title') or n.get('text') for n in news][:5],
            'balances': balances
        }
        prompt = (
            "You are a trading decision engine. Based on the provided compact JSON, choose one action: buy/sell/hold. "
            "Return JSON EXACTLY with keys: action (buy/sell/hold), pct (int 0-100), confidence (0.0-1.0), stop_rel (ATR multiple or absolute price or null), reason (short).\n\n"
            f"DATA:{json.dumps(p)}"
        )
        return prompt

    def _parse_decision_text(self, text: str) -> Dict[str, Any]:
        # Attempt to extract JSON from returned text
        try:
            # naive approach: find first "{" and last "}" and parse
            start = text.find('{')
            end = text.rfind('}')
            if start != -1 and end != -1:
                j = json.loads(text[start:end+1])
                # normalize keys
                return {
                    'action': j.get('action', 'hold'),
                    'pct': int(j.get('pct', 0)),
                    'confidence': float(j.get('confidence', 0.0)),
                    'stop_rel': j.get('stop_rel'),
                    'reason': j.get('reason', '')
                }
        except Exception:
            logger.exception('Failed to parse LLM decision JSON')
        # fallback
        return {'action': 'hold', 'pct': 0, 'confidence': 0.0, 'stop_rel': None, 'reason': 'parse_error'}
