# Quant Research Stack

WRDS 기반 기업 패널과 FRED/Cboe/FMP 외생 신호를 결합해 미국 주식 월간 멀티팩터 전략을 구성하는 최소 동작 프로젝트다.

## 포함 범위

- `Compustat Quarterly`: 분기 재무제표
- `CRSP Daily`: 일별 수익률, 시가총액, 상장폐지 수익률
- `IBES Summary`: 컨센서스, 추정치 수, 분산
- `IBES Surprise`: 실제 EPS 대비 컨센서스 서프라이즈
- `KPSS Patent`: 특허 수, 인용 수
- `Fama-French`: 시장/사이즈/가치/모멘텀 벤치마크
- `FRED DGS10`: 미국 10년 금리
- `Cboe VIX`: 변동성 지수
- `FMP Upgrades/Downgrades`: 애널리스트 투자등급 변화

## 빠른 시작

1. `config/example_config.json`을 기준으로 입력 경로와 API 키를 채운다.
2. 필요한 CSV를 `data/` 아래에 배치한다.
   FMP 실시간 수집을 쓸 경우 `api.fmp_symbols`에 유니버스를 넣는다.
3. 아래 명령으로 외부 데이터 수집과 백테스트를 실행한다.

```bash
python run_quant.py fetch --config config/example_config.json
python run_quant.py wrds-export --config config/example_config.json --dry-run
python run_quant.py wrds-export --config config/example_config.json --step compustat_quarterly
python run_quant.py signals --config config/example_config.json
python run_quant.py backtest --config config/example_config.json
python run_quant.py backtest --config config/example_config.json --no-cache
```

샘플 데이터로 즉시 검증하려면:

```bash
python run_quant.py backtest --config config/sample_config.json
```

## Outputs

- `output/rebalance_signals.csv`
- `output/portfolio_rebalances.csv`
- `output/portfolio_daily_returns.csv`
- `output/summary.json`
- `output/cache/prepared_data.pkl`

## Current Portfolio Logic

- `rdq`가 있으면 공시일 기준으로 리밸런싱 월을 잡고, 없으면 `report_lag_days`를 사용합니다.
- `sector_neutral=true`이면 섹터별 버킷에서 상위 종목을 나눠 뽑아 과도한 섹터 쏠림을 줄입니다.
- `beta_neutral=true`이면 종목별 추정 베타를 사용합니다.
- 롱온리에서 `benchmark_hedge=true`이면 `__BENCH__` 헤지 가중치가 추가됩니다.
- 롱숏에서는 숏 레그를 스케일해 포트폴리오 총 베타를 0에 가깝게 맞춥니다.
- `constraint_neutral=true`이면 롱숏 선택 종목에 대해 `beta + size + sector` 제약 투영을 적용합니다.
- 기본 실행 시 `cache_hit=0` 또는 `cache_hit=1`이 먼저 출력되고, 입력 파일이 안 바뀌면 준비된 패널을 재사용합니다.
- `transaction_cost_bps` 같은 백테스트 전용 설정 변경은 prepared-data cache를 무효화하지 않습니다.

## 참고 소스

- FRED API `fred/series/observations`: https://fred.stlouisfed.org/docs/api/fred/series/series_observations.html
- FRED DGS10 시리즈: https://fred.stlouisfed.org/series/DGS10
- Cboe VIX historical data: https://www.cboe.com/tradable_products/vix/vix_historical_data/
- FMP upgrades/downgrades: https://site.financialmodelingprep.com/developer/docs/upgrades-and-downgrades-api
- WRDS IBES-CRSP linking note: https://wrds-www.wharton.upenn.edu/pages/wrds-research/database-linking-matrix/linking-ibes-with-crsp/

## WRDS 직접 추출

- 단계별 문서: `docs/wrds_workflow.md`
- SQL 템플릿: `sql/wrds/`
- 실행 명령: `python run_quant.py wrds-export --config config/example_config.json`
- 플레이스홀더 검증: `python run_quant.py wrds-export --config config/example_config.json --dry-run`
