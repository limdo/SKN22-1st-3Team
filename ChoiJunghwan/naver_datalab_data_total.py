# 전체 데이터를 입력 받아오는 경우
# 장점: 데이터를 활용하여 다른 차들과의 검색 트랜드를 한번에 확인 할 수 있음
# 단점: 데이터 취합 하는 시간이 오래 걸림

# 네이버 검색지수 단위(=Index) 설명
# 네이버는 특정 기간에서 검색량이 가장 많은 시점을 100으로 설정하고
# 나머지는 그 기준에 따라 비율로 환산해줘.
# 예를 들어:
# 그 기간 동안 특정 키워드가 가장 많이 검색된 날이 100
# 검색량이 절반이면 → 50
# 1이면 → 최고검색량의 1/100 수준
# 즉, 1은 실제 검색량 1건이 아니라 ‘최대 검색량 대비 1% 수준’이라는 의미에 가까워.

# fetch_datalab.py
import requests
import json
import csv
import os
from dotenv import load_dotenv


load_dotenv()

NAVER_CLIENT_ID = os.getenv("NAVER_CLIENT_ID")
NAVER_CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET")

if not NAVER_CLIENT_ID or not NAVER_CLIENT_SECRET:
    raise ValueError

url = "https://openapi.naver.com/v1/datalab/search"

headers = {
    "X-Naver-Client-Id": NAVER_CLIENT_ID,
    "X-Naver-Client-Secret": NAVER_CLIENT_SECRET,
    "Content-Type": "application/json"
}

# 브랜드별 차종 정리

HYUNDAI_MODELS = [
    "아반떼", "그랜저", "싼타페", "쏘나타 디 엣지", "투싼",
    "디 올 뉴 팰리세이드", "포터2", "코나", "스타리아", "아이오닉 5",
    "캐스퍼 일렉트릭", "아이오닉 9", "디 올 뉴 넥쏘", "포터2 일렉트릭", "베뉴",
    "캐스퍼", "코나 일렉트릭", "더 뉴 아이오닉6", "ST1", "아반떼 N",
    "아이오닉 5 N"
]

KIA_MODELS = [
    "쏘렌토", "카니발", "스포티지", "셀토스",
    "K5", "봉고 3", "레이", "K8", "모닝",
    "PV5", "EV3", "EV5", "니로", "레이 EV",
    "EV6", "EV4", "타스만", "EV9", "K9", "니로 EV"
]

ALL_MODELS = HYUNDAI_MODELS + KIA_MODELS


def get_brand(car_name: str) -> str:
    if car_name in HYUNDAI_MODELS:
        return "HYUNDAI"
    elif car_name in KIA_MODELS:
        return "KIA"
    else:
        return "UNKNOWN"


# 브랜드별 차량 키워드에 대한 데이터 수집 (데이터 수집이 오래 걸리면 기간 수정 필요)
def fetch_trend(car_name):

    keyword_groups = [{"groupName": car_name, "keywords": [car_name]}]
    brand_name = get_brand(car_name)

    genders = {"male": "m", "female": "f"}
    devices = {"pc": "pc", "mobile": "mo"}

    records = []

    for gender_label, gender_code in genders.items():
        for device_label, device_code in devices.items():

            body = {
                "startDate": "2023-01-01",
                "endDate": "2025-10-31",
                "timeUnit": "month",
                "keywordGroups": keyword_groups,
                "device": device_code,
                "gender": gender_code
            }

            response = requests.post(url, headers=headers, json=body)
            response.raise_for_status()
            data = response.json()

            for result in data["results"]:
                for row in result["data"]:
                    records.append({
                        "date": row["period"],
                        "brand": brand_name,
                        "product": car_name,
                        "device": device_label,
                        "gender": gender_label,
                        "query": row["ratio"]
                    })

    print(f"{car_name} 데이터 수집 완료 ({len(records)} rows)")
    return records



# 데이터 수집
if __name__ == "__main__":

    all_records = []

    print("데이터 수집 중...\n")

    for car in ALL_MODELS:
        try:
            records = fetch_trend(car)
            all_records.extend(records)
        except Exception as e:
            print(f"{car} 조회 중 오류 발생 → {e}")

# CSV 저장
    csv_filename = "naver_datalab_brand_models_2025.csv"

    with open(csv_filename, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["date", "brand", "product", "device", "gender", "query"]
        )
        writer.writeheader()
        writer.writerows(all_records)

# 결과 내용 출력
    print(f"\n 전체 차량 CSV 저장 완료 → {csv_filename}")
    print(f"총 레코드 수: {len(all_records)}")
