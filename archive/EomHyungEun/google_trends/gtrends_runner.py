# gtrends_runner.py
import multiprocessing
import random
import time
from test import fetch_trends

# 키워드 그룹 정의 (2개씩 묶기)
KEYWORD_GROUPS = [
    ["아반떼", "싼타페"],

]

def run_group(group_keywords):
    """하나의 그룹(프로세스 단위) 실행"""
    start_delay = random.uniform(5, 60)  # 시작 지연 (IP 요청 분산)
    print(f"\n🚀 Starting group {group_keywords} (delay {start_delay:.1f}s)")
    time.sleep(start_delay)

    fetch_trends(keyword_list=group_keywords, timeframe="today 1-m", geo="KR")

def main():
    processes = []
    for group in KEYWORD_GROUPS:
        p = multiprocessing.Process(target=run_group, args=(group,))
        p.start()
        processes.append(p)

    # 모든 프로세스 대기
    for p in processes:
        p.join()

    print("\n✅ 모든 그룹 처리 완료")

if __name__ == "__main__":
    main()
