# ==========================================================
# collect_stock.py
# ----------------------------------------------------------
# 목적:
#   - 대신증권 Creon API를 이용해 모든 상장 종목의 1분봉 데이터를
#     최근 2년치(버퍼 1주 포함)까지 자동 수집 및 CSV 저장
# 특징:
#   - 종목코드 저장된 CSV기반으로 동작
#   - 중간 재시작 가능(체크포인트 기반)
#   - 일봉 보장 로직 작성
#   - 수집속도를 포기하고 대신증권 API 호출회수 고려(13회/60s)
#
# 출력:
#   out_csv/ 아래 {종목코드}_1min_2y.csv 파일 생성
#   checkpoint.json, 수집 코드 목록과 진행 인덱스 파일(없으면 생성)
# ==========================================================

import os, re, json, time, datetime as dt
from pathlib import Path
from typing import Optional, List, Dict
from collections import deque
import pandas as pd
import win32com.client
from pywintypes import com_error

import sys
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(line_buffering=True, write_through=True)
# ----------------- 경로 설정 ---------------------
BASE_DIR = Path(__file__).resolve().parent
META_DIR = BASE_DIR / "split_meta_market"
KOSPI_CSV  = META_DIR / "yahoo_meta_kospi.csv"
KOSDAQ_CSV = META_DIR / "yahoo_meta_kosdaq.csv"

OUT_DIR = BASE_DIR / "out_csv"
OUT_DIR.mkdir(exist_ok=True)
_REQ_TS = deque()  # type: deque
_MAX_CALLS = 13
_WINDOW_SEC = 60.0

CHECKPOINT = BASE_DIR / "checkpoint.json"
# ----------------- 경로 설정-END------------------

# ----------------- Cybos 객체 -----------------@대신증권
cy = win32com.client.Dispatch("CpUtil.CpCybos")
if cy.IsConnect == 0:
    raise SystemExit("Cybos 미연결: 보라색 아이콘, 관리자권한을 확인하세요.")
codemgr = win32com.client.Dispatch("CpUtil.CpCodeMgr")


# -----------------체크포인트---------------------
def load_cp() -> Dict:
    """마지막 세이브 포인트에서 재시작"""
    if CHECKPOINT.exists():
        try:
            return json.loads(CHECKPOINT.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"index": 0, "codes": []}

def save_cp(idx: int, codes: List[str]):
    """현재 진행 인덱스, 잔여 코드 json 저장"""
    CHECKPOINT.write_text(json.dumps({"index": idx, "codes": codes}, ensure_ascii=False), encoding="utf-8")

# -----------------체크포인트-END -----------------

# ----------------- 유틸 --------------------
def to_creon_code(raw: str) -> Optional[str]:
    """대신증권 주식코드로 변환, 실패값 None, 네이버코드,야후파이넨스코드 호환"""
    if pd.isna(raw):
        return None
    s = str(raw).strip().upper()
    m = re.search(r"\((\d{1,6})\)", s)  #6자리 숫자 정규식으로 추출
    if m:
        digits = m.group(1)
    else:
        if s.endswith(".KS") or s.endswith(".KQ"):
            s = s.split(".")[0]
        digits = "".join(ch for ch in s if ch.isdigit())
    if not digits:
        return None
    return "A" + digits.zfill(6)

def is_valid_stock(creon_code: str) -> bool:
    """대신증권에 존재 여부 확인"""
    try:
        #@대신증권
        sec = codemgr.GetStockSectionKind(creon_code)  # 1: 주식
        mkt = codemgr.GetStockMarketKind(creon_code)   # 1: KOSPI, 2: KOSDAQ
        return (sec == 1) and (mkt in (1, 2))
    except Exception:
        return False

def load_codes_from_csv(path: Path) -> List[str]:
    """주식 메타정보csv에서 주식코드 추출"""
    if not path.exists():
        return []
    df = pd.read_csv(path)
    cand = [c for c in ["code","Code","symbol","Symbol","ticker","Ticker"] if c in df.columns]
    series = df[cand[0]] if cand else df.iloc[:, 0]
    out = []
    for x in series.tolist():
        c = to_creon_code(x)
        if c and is_valid_stock(c):
            out.append(c)
    # 중복 제거 & 정렬
    return sorted(set(out))

def is_minute_df(df: pd.DataFrame) -> bool:
    """종가 시간 빈도로 분봉여부 판별"""
    if df.empty or "time" not in df.columns:
        return False
    u = set(df["time"].unique())
    if not u:
        return False
    # 1530 등 소수 시각만 반복되면 (일봉 폴백) False
    if len(u) <= 5 and 1530 in u:
        return False
    return True

def month_chunks(start_date: dt.date, end_date: dt.date):
    """분봉 수집 슬라이드 생성 [start,end], date"""
    cur = start_date.replace(day=1) #해당일 1일
    while cur <= end_date:
        nxt = (cur.replace(day=28) + dt.timedelta(days=4)).replace(day=1) #다음달 넘김
        s = cur
        e = min(end_date, nxt - dt.timedelta(days=1))   #해당일 1일
        yield s, e      #파이썬 제너레이터, 반복 반환
        cur = nxt

def rate_limit_wait():
    """
    _REQ_TS: deque -> 최근 호출 타임스탬프(초)
    _MAX_CALLS = 13 -> _WINDOW_SEC동안 허용할 최대 호출
    _WINDOW_SEC = 60.0 -> 슬라이딩 윈도의 길이(초)
    -----------------------------------------------
    _MAX_CALLS길이의 _REQ_TS(큐)에 호출시간 담아서 _WINDOW_SEC 기준으로 OUT 로직 실행
    Cybos API 자체 호출 제한 크로스체크
    """
    
    now = time.time()
    # 1) 윈도우 밖(60초 경과) 타임스탬프 제거
    while _REQ_TS and (now - _REQ_TS[0]) > _WINDOW_SEC:
        _REQ_TS.popleft()

    # 2) 최대 호출량 초과시, 호출 자리 대기
    if len(_REQ_TS) >= _MAX_CALLS:
        sleep_sec = _WINDOW_SEC - (now - _REQ_TS[0]) + 0.01
        if sleep_sec > 0:
            time.sleep(sleep_sec)

        now = time.time()
        # 윈도우 밖(60초 경과) 타임스탬프 제거
        while _REQ_TS and (now - _REQ_TS[0]) > _WINDOW_SEC:
            _REQ_TS.popleft()

    # 3) Cybos 자체 리밋 준수
    try:
        #@대신증권
        remain = cy.GetLimitRemainCount(1)  # 1: 시세요청
        if remain <= 0:
            time.sleep(cy.LimitRequestRemainTime/1000.0 + 0.2)
    except Exception:
        # cy 객체 이슈가 나도 슬라이딩 윈도우만으로 방어
        pass

    # 4) 이번 호출 타임스탬프 기록
    _REQ_TS.append(time.time())

def request_minute_chunk(code: str, ymd_from: int, ymd_to: int, max_retry: int = 3) -> pd.DataFrame:
    """[ymd_from, ymd_to] 구간 1분봉 요청, 기대응답 아닐시 max_retry 만큼 재시도"""
    for r in range(max_retry):
        #@대신증권
        sc = win32com.client.Dispatch("CpSysDib.StockChart")
        # 입력 순서 고정(중요)
        sc.SetInputValue(0, code)                 # 종목
        sc.SetInputValue(1, ord('2'))             # 기간 기반
        sc.SetInputValue(3, ymd_from)             # 시작 YYYYMMDD
        sc.SetInputValue(2, ymd_to)               # 종료 YYYYMMDD
        sc.SetInputValue(4, 50000)                # 최대치
        sc.SetInputValue(5, [0,1,2,3,4,5,8])      # date,time,OHLCV
        sc.SetInputValue(6, ord('m'))             # 분봉
        sc.SetInputValue(7, 1)                    # 1분
        sc.SetInputValue(9, ord('1'))             # 수정주가

        rate_limit_wait()
        try:
            #요청@대신증권
            sc.BlockRequest()
        except com_error:
            time.sleep(0.8 + 0.5*r)
            continue
        #응답@대신증권
        cnt = sc.GetHeaderValue(3)
        if cnt <= 0:
            time.sleep(0.4 + 0.3*r)
            continue

        rows = []
        for i in range(cnt):
            rows.append([
                sc.GetDataValue(0,i),  # date
                sc.GetDataValue(1,i),  # time
                sc.GetDataValue(2,i),  # open
                sc.GetDataValue(3,i),  # high
                sc.GetDataValue(4,i),  # low
                sc.GetDataValue(5,i),  # close
                sc.GetDataValue(6,i),  # volume
            ])
        df = pd.DataFrame(rows, columns=["date","time","open","high","low","close","volume"])

        if is_minute_df(df):
            return df

        # 일봉 폴백 의심 → 짧게 쉬고 재시도
        time.sleep(0.6 + 0.5*r)

    #실패시 빈프레임 반환
    return pd.DataFrame(columns=["date","time","open","high","low","close","volume"])

def collect_1min_2years(code: str) -> pd.DataFrame:
    """1분봉 2년치 수집"""
    today = dt.date.today()

    start = today - dt.timedelta(days=365*2 + 7)  # 버퍼 1주
    end   = today - dt.timedelta(days=1)          # 당일 제외

    out = []
    
    ymd = lambda d: int(d.strftime("%Y%m%d"))   #포매팅 람다함수 선언

    #분봉 결과 보장을 위해 월->반월->일 단위 순차요청
    for s, e in month_chunks(start, end):
        df = request_minute_chunk(code, ymd(s), ymd(e))
        if df.empty:
            # 반월 분해
            mid = s + (e - s)/2
            mid = dt.date(mid.year, mid.month, mid.day)
            halves = [(s, mid), (min(mid + dt.timedelta(days=1), e), e)]
            for ss, ee in halves:
                dfx = request_minute_chunk(code, ymd(ss), ymd(ee))
                if dfx.empty and (ee - ss).days >= 1:
                    # 일 단위 분해
                    day = ss
                    while day <= ee:
                        dfd = request_minute_chunk(code, ymd(day), ymd(day))
                        if not dfd.empty:
                            out.append(dfd)
                        day += dt.timedelta(days=1)
                elif not dfx.empty:
                    out.append(dfx)
        else:
            out.append(df)

    if not out:
        return pd.DataFrame(columns=["date","time","open","high","low","close","volume"])

    df_all = pd.concat(out, ignore_index=True)
    # time 유지(분봉 보존) + 정렬 + 중복 제거
    df_all["time"] = df_all["time"].astype(int)
    df_all = df_all.sort_values(["date","time"]).drop_duplicates(subset=["date","time"])
    return df_all[["date","time","open","high","low","close","volume"]]

# ----------------- 유틸-END -----------------

# ----------------- 메인 ---------------------
def main():
    kospi = load_codes_from_csv(KOSPI_CSV)
    kosdaq = load_codes_from_csv(KOSDAQ_CSV)
    codes_all = sorted(set(kospi + kosdaq))  # List[str]

    cp = load_cp()
    if cp.get("codes") != codes_all:
        cp = {"index": 0, "codes": codes_all}

    start_idx = int(cp["index"])
    print(f"총 {len(codes_all)}개, {start_idx+1}번째부터 시작")

    for i in range(start_idx, len(codes_all)):
        code = codes_all[i]
        name = codemgr.CodeToName(code)
        out_path = OUT_DIR / f"{code}_1min_2y.csv"

        try:
            # 이미 완료된 파일은 스킵
            if out_path.exists() and out_path.stat().st_size > 0:
                print(f"[{i+1}/{len(codes_all)}] {code} {name} -> exists, skip")
                save_cp(i+1, codes_all)
                continue

            print(f"[{i+1}/{len(codes_all)}] {code} {name} -> collecting...", flush=True)
            
            #수집로직
            df = collect_1min_2years(code)

            if df.empty:
                print(" empty")
            else:
                df.to_csv(out_path, index=False)
                print(f"saved {len(df)} rows", flush=True)

        except Exception as e:
            print(f" FAILED ({e})")

        # 체크포인트 갱신(다음은 i+1부터)
        save_cp(i+1, codes_all)
        time.sleep(0.15)

# ----------------- 메인-END------------------

if __name__ == "__main__":
    main()
