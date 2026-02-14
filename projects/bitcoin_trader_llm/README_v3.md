프로젝트: Bitcoin Automated Trader (v3)

요약
- 목적: LLM 기반 신호와 포지션 할당(Allocation), TWAP 실행을 결합해 비트코인 자동매매(시뮬/실거래)를 지원하는 연구/운영용 트레이딩 파이프라인입니다.
- 주요 기능: 신호 생성(AgentDecider), 할당 평가(allocation evaluator), TWAP/Executor 실행, cron 기반 스냅샷(잔고/포지션/피처) 저장, DB 저장(MariaDB/SQLite 폴백), DRY_RUN 시뮬레이션 모드 지원

핵심 컴포넌트
1) run_cron.py
- 주기적으로 시장 데이터/잔고를 스냅샷하여 cron_runs, cron_positions, cron_features 테이블에 저장합니다.
- 실행 흐름: 데이터 수집 → 총자산(KRW 환산) 계산 → reserved pool 적용 → cron_* 테이블에 저장 → Agent 신호 생성 트리거

2) app/agent_decider.py
- LLM(또는 룰 기반)으로부터 신호를 받고 agent_intent를 포함한 suggestion을 생성합니다.
- 생성된 신호는 llm_signals 테이블에 payload_json으로 저장됩니다.
- 신호 발생 시 allocation 평가를 자동으로 호출하여 allocation_proposals 테이블에 제안 저장.

3) app/allocation.py
- evaluate_allocation(db, run_id, symbol, suggested_risk_pct, stop_pct, entry_price, orderbook)
- 목표: 제안된 위험 %와 스탑%를 바탕으로 실제 투입 가능 금액(notional)을 계산.
- 고려요소: desired_risk, stop_frac, MAX_SINGLE_ORDER_PCT, orderbook 기반 슬리피지/유동성 캡, 이미 투자된 금액, reserved pool
- 결과: allocation_proposals 레코드(제안 notional, expected_slippage, fee_estimate, scale_factor 등)

4) app/executor.py, app/order_executor.py
- Executor는 allocation_proposals를 읽어 실제 주문을 수행(또는 DRY_RUN으로 시뮬레이션)
- TWAPExecutor: 큰 주문을 여러 조각으로 나눠 지정된 기간에 걸쳐 실행
- 실행 결과는 llm_executions 테이블에 result_json(실행 로그, agent_intent 포함)으로 기록
- TWAP 요약은 twap_runs 테이블에 저장

5) app/db_logger.py
- DB 연결 및 테이블 생성(create_tables_if_missing)
- 로그/레코드 삽입 헬퍼(insert/get 등)
- 지원 테이블: llm_decision_requests, llm_signals, allocation_proposals, llm_executions, cron_runs, cron_positions, cron_features, twap_runs

6) app/data_fetcher.py
- 외부 거래소/데이터 소스에서 현재 잔고/시세를 조회
- estimate_total_krw: 잔고를 KRW로 환산하여 total_equity 산출

7) app/config.py
- 설정 변수(예: RESERVED_POOL_PCT, MAX_SINGLE_ORDER_PCT 등)를 환경변수(.env)로 오버라이드 가능

DB 및 파일 위치(주요)
- 프로젝트 폴더: projects/bitcoin_trader_llm/
- 로그: projects/bitcoin_trader_llm/logs/
- 주요 코드: projects/bitcoin_trader_llm/app/
- .env: projects/bitcoin_trader_llm/.env (DB 접속 정보 포함 — 민감 정보는 커밋 금지)

동작 흐름(요약)
1. run_cron.py(크론 또는 수동 실행) 시작
2. DataFetcher가 시세 및 잔고 수집 → total_equity 계산
3. cron_* 테이블에 스냅샷 저장
4. AgentDecider가 pending decision_requests를 처리하여 llm_signals 생성
5. allocation.evaluate_allocation 호출 → allocation_proposals 생성
6. Executor가 allocation_proposals를 읽고(또는 TWAP으로) 주문 실행/시뮬 → llm_executions, twap_runs에 기록

간단한 ASCII 다이어그램

[run_cron] --> [DataFetcher] --> [cron_runs/cron_positions/cron_features]
                             |
                             v
                       [AgentDecider]
                             |
                             v
                   [allocation.evaluate_allocation]
                             |
                             v
                    [allocation_proposals (DB)]
                             |
                             v
                          [Executor]
                             |
            -----------------|----------------
            |                                |
            v                                v
      [TWAPExecutor]                  [OrderExecutor]
            |                                |
            v                                v
      [twap_runs (DB)]               [llm_executions (DB)]

테스트/운영 모드
- DRY_RUN: 시뮬레이션 모드로 실제 주문을 전송하지 않음. 슬리피지/수수료 추정과 가상 체결로 결과를 기록.
- REAL: 실제 거래소 API 키를 .env에 설정하고 REAL 모드로 실행하면 실제 주문 전송(권장: 소규모로 먼저 테스트)

권장 작업(우선순위)
1. README 문서(현재 파일) 검토 및 배포 허용
2. v3 PR에 리뷰 코멘트 추가(내가 PR 본문 초안도 만들어줄게요 원하면)
3. DRY_RUN 5회(소규모 스모크) → 결과 확인
4. DRY_RUN 50회(스트레스) → 통계/슬리피지 리포트

주의사항
- .env에 민감 정보(API 키, DB 비밀번호)를 절대 커밋하지 마세요.
- 실거래 모드 전에는 소규모 DRY_RUN과 스톱-로스/리스크 파라미터 재검증을 권장합니다.

문서/다이어그램 추가 요청사항
- 추가로 포함하길 원하는 다이어그램(시퀀스/ERD/인프라 토폴로지)이나 예시 .env 템플릿 등을 알려주시면 반영하겠습니다.

작성자: 예원 (assistant)
작성일: 2026-02-15
