# ==========================================================
# collect_krx_naver_symbols_v2.py
# ----------------------------------------------------------
# 목적 및 특징:
#   - 네이버 금융 시가총액 페이지에서 KOSPI, KOSDAQ 상장 종목 코드/이름 수집
#   - Yahoo Finance 호환 심볼 형식(.KS / .KQ)으로 변환 후 CSV 저장
#   - requests + BeautifulSoup 기반의 비공식 웹 크롤링
#
# 출력:
#   split_meta_market/
#       ├── naver_stock_list_yahoo_format.csv   (전체)
#       ├── yahoo_meta_kospi.csv               (KOSPI 전용)
#       └── yahoo_meta_kosdaq.csv              (KOSDAQ 전용)
# ==========================================================

import re
import time
import os
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://finance.naver.com/",
}
BASE_URL = "https://finance.naver.com/sise/sise_market_sum.naver?sosok={market}&page={page}"

def requests_session():
    s = requests.Session()
    retries = Retry(
        total=6, backoff_factor=0.4,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update(HEADERS)
    return s

def collect_market(market: int, max_pages: int = 200, sleep_sec: float = 0.15, empty_tolerance: int = 5):
    """
    market: 0=KOSPI, 1=KOSDAQ
    정규식으로 code=###### 전부 수집. 연속 empty_tolerance 페이지가 비면 종료.
    """
    sess = requests_session()
    market_name = "KOSPI" if market == 0 else "KOSDAQ"

    codes_set = []          # 순서 유지
    code_seen = set()
    code2name = {}

    empty_count = 0
    for page in range(1, max_pages + 1):
        url = BASE_URL.format(market=market, page=page)
        r = sess.get(url, timeout=15)
        r.encoding = "euc-kr"
        html = r.text

        # 1) 코드는 정규식으로 전수 수집 (누락 최소화)
        codes = re.findall(r"code=(\d{6})", html)

        # 2) 이름은 앵커에서만 매핑(있으면 사용)
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.select("a.tltle"):
            href = a.get("href", "")
            m = re.search(r"code=(\d{6})", href)
            if not m:
                continue
            c = m.group(1)
            name = (a.get_text() or "").strip()
            if c and name:
                code2name[c] = name

        # 페이지가 비었는지 판정
        unique_new = [c for c in codes if c not in code_seen]
        if not unique_new:
            empty_count += 1
            if empty_count >= empty_tolerance:
                break
        else:
            empty_count = 0

        # 누적
        for c in unique_new:
            code_seen.add(c)
            codes_set.append(c)

        time.sleep(sleep_sec)

    # DataFrame 구성
    df = pd.DataFrame({"종목코드": [c for c in codes_set]})
    df["시장"] = market_name
    df["종목명"] = df["종목코드"].map(code2name).fillna("")
    df["YahooSymbol"] = df.apply(
        lambda x: f"{x['종목코드']}.KS" if x["시장"] == "KOSPI" else f"{x['종목코드']}.KQ",
        axis=1
    )
    df = df.drop_duplicates(subset=["YahooSymbol"]).reset_index(drop=True)
    return df

def main():
    print("KOSPI 수집...")
    df_kospi = collect_market(0, max_pages=200, empty_tolerance=5)
    print(f"  → {len(df_kospi)} rows")

    print("KOSDAQ 수집...")
    df_kosdaq = collect_market(1, max_pages=240, empty_tolerance=5)
    print(f"  → {len(df_kosdaq)} rows")

    df_total = pd.concat([df_kospi, df_kosdaq], ignore_index=True)
    df_total = df_total.drop_duplicates(subset=["YahooSymbol"]).reset_index(drop=True)

    # === 파일 저장 ===
    out_dir = "split_meta_market"
    os.makedirs(out_dir, exist_ok=True)

    # 전체 통합본
    total_path = os.path.join(out_dir, "naver_stock_list_yahoo_format.csv")
    df_total.to_csv(total_path, index=False, encoding="utf-8-sig")

    # 개별 시장 파일
    df_kospi.to_csv(os.path.join(out_dir, "yahoo_meta_kospi.csv"), index=False, encoding="utf-8-sig")
    df_kosdaq.to_csv(os.path.join(out_dir, "yahoo_meta_kosdaq.csv"), index=False, encoding="utf-8-sig")

    print(f"저장 완료: {total_path}")
    print(f"총 종목수: {len(df_total)} (KOSPI={len(df_kospi)}, KOSDAQ={len(df_kosdaq)})")

if __name__ == "__main__":
    main()
