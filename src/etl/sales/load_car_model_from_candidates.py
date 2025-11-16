# src/etl/sales/load_car_model_from_candidates.py

import csv
from pathlib import Path

from sqlalchemy import text

# 프로젝트의 DB 연결 함수
from src.db.connection import get_engine


# ----------------------------------------
# 경로 설정
# ----------------------------------------

BASE_DIR = Path(__file__).resolve().parents[3]  # 프로젝트 루트
CANDIDATES_PATH = BASE_DIR / "data" / "raw" / "car_model_candidates.csv"


# ----------------------------------------
# CSV 로드 함수
# ----------------------------------------


def load_candidates():
    if not CANDIDATES_PATH.exists():
        raise FileNotFoundError(
            f"[ERROR] {CANDIDATES_PATH} 파일이 없습니다. 먼저 extract_car_model_candidates.py를 실행하세요."
        )

    with CANDIDATES_PATH.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row


# ----------------------------------------
# car_model UPSERT
# ----------------------------------------


def upsert_car_model():
    engine = get_engine(echo=False)

    with engine.begin() as conn:
        for row in load_candidates():
            brand_name = row["brand_name"].strip()
            model_name_kr = row["model_name_kr"].strip()

            # 이미 같은 모델이 있으면 삽입 생략
            existing = conn.execute(
                text(
                    """
                    SELECT model_id
                    FROM car_model
                    WHERE brand_name = :brand_name
                      AND model_name_kr = :model_name_kr
                    """
                ),
                {"brand_name": brand_name, "model_name_kr": model_name_kr},
            ).fetchone()

            if existing:
                # 이미 등록된 모델이면 건너뛰기
                continue

            # 신규 모델 INSERT (danawa_model_id, danawa_model_url은 NULL)
            conn.execute(
                text(
                    """
                    INSERT INTO car_model (
                        danawa_model_id,
                        brand_name,
                        model_name_kr,
                        danawa_model_url
                    )
                    VALUES (
                        NULL,
                        :brand_name,
                        :model_name_kr,
                        NULL
                    )
                    """
                ),
                {"brand_name": brand_name, "model_name_kr": model_name_kr},
            )

    print("[OK] car_model 테이블 적재 완료!")


# ----------------------------------------
# main
# ----------------------------------------


def main():
    print(f"[INFO] 후보 파일: {CANDIDATES_PATH}")
    upsert_car_model()


if __name__ == "__main__":
    main()
