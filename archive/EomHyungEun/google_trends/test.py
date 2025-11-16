# fetch_trends_worker.py
from pytrends.request import TrendReq
import pandas as pd
import random, time, traceback

def fetch_trends(keyword_list, timeframe="today 1-m", geo="KR", max_retries=3):
    """
    특정 키워드 그룹에 대한 구글 트렌드 데이터 수집
    (429 대응, 랜덤 sleep, 지수 백오프 포함)
    """
    attempt = 0
    while attempt < max_retries:
        try:
            print(f"\n[TRY {attempt+1}] Fetching {keyword_list}...")
            pytrends = TrendReq(hl='ko', tz=540)
            pytrends.build_payload(keyword_list, timeframe=timeframe, geo=geo)

            df = pytrends.interest_over_time()
            if df.empty:
                print(f"❌ No data for {keyword_list}")
                return None

            filename = f"{'_'.join(keyword_list)}.csv"
            df.to_csv(filename, index=True)
            print(f"✅ Saved {filename} ({len(df)} rows)")
            return filename

        except Exception as e:
            msg = str(e)
            print(f"⚠️ Error: {msg}")
            traceback.print_exc()

            # 429(TooManyRequests) 감지 시 대기 시간 증가
            if "429" in msg or "TooManyRequests" in msg:
                delay = random.uniform(60, 180) * (attempt + 1)
                print(f"⏸ 429 감지 — {delay:.1f}초 대기 후 재시도")
                time.sleep(delay)
            else:
                # 기타 에러는 짧게 대기
                delay = random.uniform(10, 30)
                print(f"⏸ 일반 에러 — {delay:.1f}초 대기 후 재시도")
                time.sleep(delay)
        attempt += 1

    print(f"❌ 모든 재시도 실패: {keyword_list}")
    return None
