# ==========================================================
# runner_watch.py
# ----------------------------------------------------------
# 목적:
#   - collect_stock.py 수집 실패시 블로킹 상태 해결을 위해 작성
#   - 비블로킹 로그 스트림을 통해 진행 상태 추적 및 타임아웃 감지
#
# 특징:
#   - subprocess.Popen으로 자식 프로세스 생성 (CREON 수집 프로세스)
#   - 전용 스레드(reader_thread)로 stdout을 실시간 큐(queue)에 적재
#   - 메인 스레드는 로그를 소비하며 상태머신 방식으로 진행 관리
#   - “-> collecting…” 이후 TIMEOUT_SEC 동안 “saved N rows” 미출력 시 타임아웃 판정
#   - 타임아웃 발생 시 자식 프로세스 kill 후 재시작
#   - KeyboardInterrupt 시 전체 종료 (정상종료는 collect_stock.py 내부 처리)
#
# 출력:
#   - 실시간 stdout 중계 및 stderr에 디버그 로그 출력
#   - 타임아웃 시 "[TIMEOUT]" 로그, 정상 종료 시 "[OK]" 로그 표시
# ==========================================================

import os, sys, time, threading, queue, subprocess, signal, re
from pathlib import Path

SCRIPT = "collect_stock.py"   # 감시 대상 스크립트 파일명 (collect_stock.py)
TIMEOUT_SEC = 240             # collecting 이후 최대 무응답 허용 시간(s)
RETRY_DELAY = 15              # 재시작 전 대기 시간
MAX_RESTARTS = 0              # 재시작 한도 (0이면 무제한)

# 진행 상태를 판별하기 위한 출력 패턴
RE_STARTLINE  = re.compile(r"^\[\s*\d+\s*/\s*\d+\s*\]")           # “[ 134 / 2677 ] ...” 형태 감지 (새 종목 시작)
RE_COLLECTING = re.compile(r"->\s*collecting", re.IGNORECASE)# “-> collecting...” 라인 감지

RE_SAVED      = re.compile(r"\bsaved\s+\d[\d,]*\s+rows\b", re.IGNORECASE)# “saved N rows” 감지
RE_SAVED_ROWS = re.compile(r"saved\s+(\d+)\s+rows", re.IGNORECASE) # “saved 72161 rows”에서 저장된 행 수 추출

# ----- 유틸 -----------
def kill_process(proc):
    """자식 프로세스를 안전하게 종료"""
    if proc.poll() is not None:
        return  # 이미 종료됨
    try:
        if os.name == "nt":
            proc.send_signal(signal.CTRL_BREAK_EVENT)  # 소프트 종료 시도
            time.sleep(1.0)
            proc.kill()                                # 그래도 살아있으면 강제 종료
    except Exception:
        # 어떤 예외가 나더라도 최후의 시도로 kill
        try:
            proc.kill()
        except Exception:
            pass

def reader_thread(proc, q: queue.Queue):
    """자식 프로세스 stdout 처리할 runner_watch 소비 큐"""
    try:
        for line in iter(proc.stdout.readline, ""):
            q.put(line)
    finally:
        q.put(None)  # 출력 종료(프로세스 종료) 표시

# ----- 유틸-END--------

def spawn_process():
    """자식 프로세스 생성 및 출력 파이프 설정"""
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"   # 자식 프로세스 stdout 버퍼링 최소화(라인 실시간 전달)
    env["PYTHONIOENCODING"] = "utf-8"

    # 프로세스 중지 시작을 위한 프로세스 그룹 지정
    creationflags = 0
    if os.name == "nt":
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP

    return subprocess.Popen(
        [sys.executable, "-u", SCRIPT],
        stdout=subprocess.PIPE,          # 표준출력을 파이프로
        stderr=subprocess.STDOUT,        # stderr도 합쳐서 단일 스트림으로
        bufsize=1,                       # line-buffered
        universal_newlines=True,         # 텍스트 모드(줄 단위 처리)
        encoding="utf-8",                # 인코딩 설정 강제(보완용)
        errors="replace",
        env=env,
        creationflags=creationflags
    )

def main():
    restarts = 0

    while True:
        print(f"[RUN] {SCRIPT} 시작 (타임아웃 {TIMEOUT_SEC//60}분)")
        
        # === 파이프 생성 및 프로세스 실행  ===
        proc = spawn_process()
        q: queue.Queue[str | None] = queue.Queue()
        t = threading.Thread(target=reader_thread, args=(proc, q), daemon=True)
        t.start()

        # === STDOUT 담은 소비 큐 기준 처리  ===

        in_progress = False
        exited_normally = False     #runnser_watch 종료 플래그

        while True:

            try:
                # 큐 수집( per 1s )
                try:
                    item = q.get(timeout=1.0)
                except queue.Empty:
                    item = ""

                now = time.time()

                # 자식 프로세스 EOF시 종료
                if item is None:
                    proc.wait(timeout=3)
                    exited_normally = (proc.returncode == 0)
                    break

                if item:
                    # 자식 프로세스 콘솔 중계
                    sys.stdout.write(item)
                    sys.stdout.flush()

                    current = now
                    ts = time.strftime("%H:%M:%S")

                    # 디버깅용 시작 플래그 탐색(“[ 134 / 2677 ] ...”), 타이머 x
                    if RE_STARTLINE.search(item):
                        collect_start_ts = current
                        in_progress = False
                        print(f"[DBG {ts}] startline detected; timer armed", file=sys.stderr, flush=True)

                    # 'saved' 우선 판정
                    has_saved   = RE_SAVED.search(item) is not None
                    has_collect = RE_COLLECTING.search(item) is not None

                    # 종료 플래그 탐색(“saved N rows”)
                    if has_saved:
                        in_progress = False
                        collect_start_ts = None
                        m_rows = RE_SAVED_ROWS.search(item)
                        rows = m_rows.group(1) if m_rows else "?"
                        elapsed = (current - collect_start_ts) if collect_start_ts else 0.0
                        print(f"[DBG {ts}] saved detected; rows={rows}, elapsed={elapsed:.1f}s", file=sys.stderr, flush=True)
                    # 시작 플래그 탐색(“-> collecting...”)
                    elif has_collect:
                        in_progress = True
                        collect_start_ts = current
                        print(f"[DBG {ts}] collecting detected; timer (re)started", file=sys.stderr, flush=True)

                # 타임아웃 판정: collecting 이후 TIMEOUT_SEC 동안 'saved'가 안 오면 재시작
                if in_progress and collect_start_ts is not None:
                    if now - collect_start_ts > TIMEOUT_SEC:
                        print(f"[TIMEOUT] {TIMEOUT_SEC//60}분 무응답 → 프로세스 재시작")
                        kill_process(proc)
                        break


                # 자발 종료(정상/비정상) 감지
                if proc.poll() is not None:
                    exited_normally = (proc.returncode == 0)
                    break

            except KeyboardInterrupt:
                # 사용자가 runner 자체를 중단
                print("\n[STOP] 사용자 중단")
                kill_process(proc)
                return
            except Exception as e:
                # runner 자체 예외: 자식 정리 후 재시작 루프로 복귀
                print(f"[ERROR] runner 예외: {e}")
                kill_process(proc)
                break

        # === 실행 종료 후 처리 ===
        if exited_normally:
            # collect_stock.py가 스스로 정상 종료(예: 모든 수집 완료)
            print("[OK] 정상 종료 → runner 종료")
            return

        # 재시작 카운팅/한도
        restarts += 1
        if MAX_RESTARTS and restarts > MAX_RESTARTS:
            print(f"[ABORT] 재시작 한도 초과({MAX_RESTARTS}) → 종료")
            return

        # 재시작 대기 및 루프 계속
        print(f"[RESTART] {RETRY_DELAY}s 후 재시작 (누적 {restarts})")
        time.sleep(RETRY_DELAY)

if __name__ == "__main__":
    main()
