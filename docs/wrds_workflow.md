# WRDS Direct Extraction Workflow

이 문서는 2026-04-09 기준 WRDS 제품 구조를 기준으로, 현재 프로젝트가 기대하는 CSV를 단계별로 만드는 절차를 정리한다.

## 핵심 원칙

- 기업 재무 축은 `gvkey`
- 주가 축은 `permno`
- 재무-주가 연결은 `CCM`
- 애널리스트 축은 `IBES ticker -> permno`
- 거시 축은 `date`
- CRSP는 2025년 이후 `CIZ` 전환이 진행 중이므로, 기관 환경에 따라 legacy-style view 또는 CIZ 호환 테이블명을 써야 한다.

## 단계

1. Compustat Quarterly 추출
   파일: `sql/wrds/01_compustat_quarterly.sql`
   출력: `data/compustat_quarterly.csv`
   권장 필드: `rdq`, `sic` 또는 `gsector`

2. CCM Link 추출
   파일: `sql/wrds/02_ccm_link.sql`
   출력: `data/ccm_link.csv`

3. CRSP Daily 추출
   파일: `sql/wrds/03_crsp_daily.sql`
   출력: `data/crsp_daily.csv`

4. IBES-CRSP Link 생성
   파일: `sql/wrds/04_ibes_link_template.sql`
   출력: `data/ibes_crsp_link.csv`

5. IBES Summary 추출
   파일: `sql/wrds/05_ibes_summary_template.sql`
   출력: `data/ibes_summary.csv`

6. IBES Surprise 추출
   파일: `sql/wrds/06_ibes_surprise_template.sql`
   출력: `data/ibes_surprise.csv`

7. KPSS Patent 추출
   파일: `sql/wrds/07_kpss_patent_template.sql`
   출력: `data/kpss_patent.csv`

8. Fama-French Factors 추출
   파일: `sql/wrds/08_ff_factors_template.sql`
   출력: `data/ff_factors.csv`

9. WRDS SQL 검증 및 실행
   `python run_quant.py wrds-export --config config/example_config.json --dry-run`
   `python run_quant.py wrds-export --config config/example_config.json`

10. 외부 API/FRED/Cboe/FMP 보강
   `python run_quant.py fetch --config config/example_config.json`

11. 전체 백테스트 실행
   `python run_quant.py backtest --config config/example_config.json`

## 실제 연결 순서

1. `compustat_quarterly`를 `gvkey, datadate` 기준으로 준비한다.
   가능하면 `rdq`를 함께 받아 실제 공시일 기준 리밸런싱에 쓴다.
2. `ccm_link`를 붙여 `permno`를 확보한다.
3. `crsp_daily`를 `permno, date` 기준으로 붙여 일별 수익률과 시가총액을 만든다.
4. `ibes_crsp_link`를 붙여 `ticker -> permno` 축을 만든다.
5. `ibes_summary`, `ibes_surprise`를 `ticker, statpers` 기준으로 붙인다.
6. `kpss_patent`를 다시 `gvkey` 기준으로 붙인다.
7. `fred_dgs10`, `cboe_vix`를 날짜별 시장 공통 신호로 붙인다.
8. `fmp_grades`를 `symbol/ticker` 기준으로 붙여 최근 업/다운그레이드 펄스를 만든다.

## 주의

- `datadate` 직후 즉시 매수하면 룩어헤드가 생긴다. 실제 논문/실거래용이면 공시 지연을 추가해야 한다.
- IBES summary/surprise 테이블명은 기관 구독 범위와 WRDS 라이브러리 노출 방식에 따라 다를 수 있다. 템플릿의 플레이스홀더를 본인 환경에 맞게 치환해야 한다.
- CRSP daily는 일부 기관에서 여전히 `dsf/dse` 호환 뷰를 제공하지만, 최신 구조는 CIZ다. 2025년 2월 이후 신규 릴리스는 CIZ 기준이라는 점을 먼저 확인해야 한다.
- `wrds.host`, `wrds.port`, `wrds.database` 기본값은 일반적인 WRDS PostgreSQL 접속값을 기준으로 둔 추정값이다. 실제 기관 설정이 다르면 config에서 덮어써야 한다.
- 실제 접속 전 환경변수 `WRDS_USERNAME`, `WRDS_PASSWORD`를 설정해야 한다.

## 공식 참고

- WRDS Linking Compustat with CRSP: https://wrds-www.wharton.upenn.edu/pages/wrds-research/database-linking-matrix/linking-compustat-with-crsp/
- WRDS Linking IBES with CRSP: https://wrds-www.wharton.upenn.edu/pages/wrds-research/database-linking-matrix/linking-ibes-with-crsp/
- WRDS ICLINK macro: https://wrds-www.wharton.upenn.edu/pages/wrds-research/macros/wrds-macro-iclink/
- WRDS ICLINK CIZ update: https://wrds-www.wharton.upenn.edu/pages/wrds-research/macros/wrds-macro-iclink-ciz/
- WRDS CRSP CIZ transition notice: https://wrds-www.wharton.upenn.edu/pages/data-announcements/changes-to-crsp-data/
- WRDS Fama-French product page: https://wrds-www.wharton.upenn.edu/pages/about/data-vendors/fama-french-portfolios-factors/
