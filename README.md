# MEXC AI Market Monitor

MEXC USDT 무기한 선물의 전체 티커를 감시하고, 거래대금 상위 종목에 대해 멀티타임프레임 캔들을 추적한 뒤 조건 변화가 있을 때 OpenAI가 한국어 시장 구조 분석을 생성하는 **분석 전용** 프로젝트입니다.

## 중요한 설계 범위

- 전체 USDT 무기한 계약: MEXC의 글로벌 `sub.tickers` WebSocket으로 가격, 24시간 거래량/거래대금 등을 감시합니다.
- 상세 캔들/AI 분석: 모든 상장 종목에 모든 시간 프레임을 개별 구독하면 구독 수, API 부하, 서버 자원이 크게 증가할 수 있습니다. 따라서 기본값은 거래대금 상위 `DETAIL_SYMBOL_LIMIT=60`개에 대해 1m/5m/15m/1h/4h 캔들 및 AI 분석을 수행합니다.
- 상세 추적 종목은 30초마다 거래대금 기준으로 다시 계산되고, 캔들 WebSocket 구독이 동적으로 추가/해제됩니다. 전체 티커 감시와 상세 캔들 분석을 구분한 이유는 전체 종목 감시를 유지하면서 안정적인 운영을 하기 위해서입니다.
- MEXC API는 공개 시장 데이터 조회에 API 키가 필요하지 않습니다. 이 프로젝트에는 거래 권한이나 API secret을 입력하지 않습니다.
- 주문/계정 조회/포지션 변경 기능은 포함하지 않습니다.

## 1. 클라우드 서버 준비

Ubuntu 22.04/24.04 VPS 권장. 최소 2 vCPU / 4GB RAM부터 시작하고, 상세 종목 수를 늘릴 경우 리소스를 모니터링하세요.

서버에 Docker와 Docker Compose를 설치한 뒤 프로젝트 폴더를 업로드합니다.

## 2. 환경변수

```bash
cp .env.example .env
nano .env
```

`OPENAI_API_KEY`에 OpenAI API 키를 입력합니다. API 키는 브라우저나 프론트엔드 코드에 넣지 않습니다.

설정 예시:

```env
OPENAI_API_KEY=your_api_key
OPENAI_MODEL=gpt-5-mini
DETAIL_SYMBOL_LIMIT=60
KLINE_INTERVALS=Min1,Min5,Min15,Min60,Hour4
AI_ENABLED=true
AI_MIN_SECONDS_BETWEEN_CALLS_PER_SYMBOL=300
AI_MAX_CALLS_PER_HOUR=120
```

모델 이름은 계정에서 API 사용이 가능한 모델명으로 바꿀 수 있습니다. 모델 접근 권한은 ChatGPT 구독과 별개이며 API 과금이 발생할 수 있습니다.

## 3. 실행

```bash
docker compose up -d --build
docker compose logs -f
```

브라우저에서 `http://서버IP:8000`으로 접속합니다.

## 4. 방화벽 / HTTPS

- 외부에 공개하기 전 방화벽에서 8000 포트를 본인 IP로 제한하거나 Caddy/Nginx를 앞에 두고 HTTPS 및 인증을 추가하세요.
- 현재 기본 예제에는 로그인/사용자 인증이 없습니다. 공개 인터넷에 그대로 노출하지 마세요.
- 운영 배포 시 반드시 HTTPS, 접근 제어, 서버 보안 업데이트를 적용하세요.

## 5. API

- `GET /api/status`: 연결 및 시스템 상태
- `GET /api/markets`: 티커 목록
- `GET /api/market/{symbol}`: 종목별 캔들 및 AI 상태
- `GET /api/analyses`: 최근 AI 분석
- `WS /ws`: 실시간 티커 및 AI 이벤트

## 6. 분석 로직

기본 구조 엔진은 직전 완료된 캔들 20개의 고가/저가 범위를 계산하고, 마지막 완료 캔들의 종가가 범위 위/아래에서 마감했는지를 기록합니다. 이는 초기 기준 로직이며, 사용자가 실제로 원하는 가격 반응 기반 지지/저항 분석으로 고도화할 수 있습니다.

- 이동평균선/피벗 지표는 사용하지 않습니다.
- 라이브 캔들 값과 완료된 캔들 값을 구분합니다.
- AI 응답은 데이터에 근거한 참고 분석이며 예측 보장이나 매매 지시가 아닙니다.
- 현재 AI 호출은 종목별 5분 쿨다운 및 시간당 전체 호출 상한으로 제한됩니다.
- API 호출 비용은 사용량과 선택 모델에 따라 발생합니다.

## 7. MEXC API 참고

- Futures integration: https://www.mexc.io/api-docs/futures/integration-guide
- Tickers WebSocket: https://www.mexc.co/api-docs/futures/websocket-api/tickers
- K-line WebSocket: https://www.mexc.co/api-docs/futures/websocket-api/k-line-data
- Contract detail / ticker REST: https://www.mexc.io/api-docs/futures/market-endpoints/get-ticker-contract-market-data

MEXC API 사양 및 제한은 변경될 수 있습니다. 운영 전 공식 문서와 실제 응답을 확인하세요.
