# 📈 KRX Minute Data Collector

**대신증권 Creon API와 네이버 금융 데이터를 이용해 한국 주식의 최근 2년치 분봉 데이터를 자동 수집하는 저장소입니다.**  
KOSPI·KOSDAQ 전 종목의 심볼 정보를 네이버에서 크롤링한 뒤, 대신증권 API를 통해 1분봉 단위로 데이터를 수집합니다.

---

## 🧩 구성

```
|-- collect_krx_naver_symbols_v2.py # 네이버 금융에서 종목 코드/이름 수집
|-- collect_stock.py                # 대신증권 API로 2년치 분봉 데이터 수집
|-- runner_watch.py                 # 수집 프로세스 감시 및 자동 재시작
|-- split_meta_market/              # 종목 메타데이터 CSV 저장 폴더
|-- out_csv/                        # 수집된 분봉 CSV 출력 폴더
|-- checkpoint.json                 # 수집 진행 상태 저장
|-- requirements.txt
|-- README.md
```

---

## ⚙️ 주요 기능

### 1️⃣ 종목 코드 수집 (`collect_krx_naver_symbols_v2.py`)
- 네이버 금융 시가총액 페이지를 크롤링하여 **KOSPI/KOSDAQ 종목 코드 및 이름 수집**
- Yahoo Finance 호환 형태(`.KS`, `.KQ`)로 변환
- 출력 파일:
  - `split_meta_market/naver_stock_list_yahoo_format.csv`
  - `split_meta_market/yahoo_meta_kospi.csv`
  - `split_meta_market/yahoo_meta_kosdaq.csv`

### 2️⃣ 분봉 데이터 수집 (`collect_stock.py`)
- **대신증권 Creon API**를 이용하여 각 종목의 **최근 2년치 1분봉 데이터 수집**
- **속도보다 안정성** 우선: API 호출 제한(13회/분) 준수
- 수집 결과:
  - `out_csv/{종목코드}_1min_2y.csv`
- 진행 상태 자동 저장 (`checkpoint.json`)
  - 중간 중단 시 재시작 가능

### 3️⃣ 자동 감시 및 복구 (`runner_watch.py`)
- `collect_stock.py` 실행 중 **무응답(timeout)** 발생 시 자동으로 재시작
- “→ collecting…” 이후 일정 시간(`TIMEOUT_SEC`) 동안 응답이 없으면 프로세스 kill 및 재시작
- `[OK]`, `[TIMEOUT]`, `[RESTART]` 로그로 상태 표시

---

## 🔧 요구 사항

- **OS**: Windows (Creon API는 Windows COM 기반, 32bit)
- **Python**: **3.8.10에서 동작 확인** (권장: 3.8.x)
- **Creon Plus**: 로그인 상태 및 연결 필요(관리자 권한 실행 권장)

## 🪜 실행 순서

1. **가상환경 활성화**
```bash
python -m venv venv
source venv/Scripts/activate  # (Windows PowerShell)
pip install -r requirements.txt
```
2. **종목 코드 수집**
```bash
python collect_krx_naver_symbols_v2.py
```

3. **분봉 데이터 수집 (대신증권 Creon API 로그인 필요)**
```
python runner_watch.py
```

## ⚠️ 주의사항

- 대신증권 Creon API는 Windows 전용 COM 기반으로, 관리자 권한 실행 필수
- Creon Plus 로그인 및 연결 상태 확인 (CpCybos.IsConnect == 1)
- API 호출 제한(13회/60초)을 우회하지 않도록 주의
- 종목 수가 많을 경우 장시간 실행 필요
- 해당 코드들은 네트워크, API 상태, 네이버 html수정에 동작하지 않을 수 있음

## 📂 출력 데이터 형식

각 파일: A000020_1min_2y.csv

```cs
date,time,open,high,low,close,volume
20231101,0900,53100,53200,53000,53100,1245
20231101,0901,53100,53300,53000,53200,874
...
```

---

### ⚙️ 외부 서비스 관련 고지

- **대신증권 Creon API**  
  - Windows 전용, 로그인 및 인증 필요  
  - API 호출 제한 및 이용 약관에 따라 사용해야 합니다  
  - 본 저장소는 비공식 예시 코드이며, 상업적 또는 자동화 거래 용도가 아닙니다  

- **네이버 금융 데이터**  
  - `robots.txt` 기준 `/sise/` 경로는 수집 허용 범위에 포함됩니다  
  - 단, 전체 사이트 크롤링은 차단되어 있으며, 데이터 재가공·재배포는 이용약관상 제한될 수 있습니다  
  - 이용 시 네이버 금융의 [이용약관](https://policy.naver.com/rules/service.html) 및 [`robots.txt`](https://finance.naver.com/robots.txt) 정책을 준수해야 합니다


## 🔗 참고한 자료

[분봉데이터 다운로드 방법(블로그)](https://velog.io/@withs-study/%EB%B6%84%EB%B4%89%EB%8D%B0%EC%9D%B4%ED%84%B0-%EB%8B%A4%EC%9A%B4%EB%A1%9C%EB%93%9C-%EB%B0%A9%EB%B2%95)
→ 대신증권 관련 세부 설정/주의사항은 위 글을 참조하면 무리 없습니다.

[Creon Datareader 예시(GitHub)](https://github.com/gyusu/Creon-Datareader)