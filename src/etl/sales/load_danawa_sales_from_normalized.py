# src/etl/sales/load_danawa_sales_from_normalized.py

import csv
import re
from pathlib import Path

from sqlalchemy import text

from src.db.connection import get_engine


# ----------------------------------------
# 경로 설정
# ----------------------------------------

BASE_DIR = Path(__file__).resolve().parents[3]  # 프로젝트 루트
DANAWA_BASE = BASE_DIR / "data" / "raw" / "danawa" / "25_11_14"


# ----------------------------------------
# 유틸 함수
# ----------------------------------------


def parse_month_from_filename(filename: str) -> str:
    """
    예: hyundai_model_sales_2024_06_00_normalized.csv → '2024-06-01'
    """
    m = re.search(r"(\d{4})_(\d{2})_00", filename)
    if not m:
        raise ValueError(f"파일명에서 연월을 찾을 수 없음: {filename}")
    year, month = m.group(1), m.group(2)
    return f"{year}-{month}-01"


def iter_normalized_files():
    """
    현대/기아 normalized CSV 전체 경로 + 브랜드 이름을 yield
    """
    for brand_name, subdir in [("현대", "hyundai"), ("기아", "kia")]:
        brand_dir = DANAWA_BASE / subdir
        if not brand_dir.exists():
            continue
        for path in sorted(brand_dir.glob("*_normalized.csv")):
            yield brand_name, path


def build_model_id_map(conn):
    """
    car_model 테이블에서 (brand_name, model_name_kr) -> model_id 맵을 미리 가져온다.
    """
    rows = conn.execute(
        text(
            """
            SELECT model_id, brand_name, model_name_kr
            FROM car_model
            """
        )
    ).fetchall()

    mapping: dict[tuple[str, str], int] = {}
    for model_id, brand_name, model_name_kr in rows:
        key = (brand_name.strip(), model_name_kr.strip())
        mapping[key] = model_id
    return mapping


# ----------------------------------------
# 메인 로직
# ----------------------------------------


def load_sales():
    engine = get_engine(echo=False)

    with engine.begin() as conn:
        model_id_map = build_model_id_map(conn)

        total_rows = 0
        inserted_rows = 0
        skipped_no_model = 0

        for brand_name, path in iter_normalized_files():
            month_date = parse_month_from_filename(path.name)
            print(f"[INFO] 처리 중: {brand_name} / {path.name} (month={month_date})")

            with path.open("r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    total_rows += 1

                    model_name = (row.get("모델명") or "").strip()
                    sales_str = (row.get("판매량") or "").strip()

                    if not model_name:
                        continue

                    try:
                        sales_units = (
                            int(sales_str.replace(",", "")) if sales_str else 0
                        )
                    except ValueError:
                        sales_units = 0

                    key = (brand_name, model_name)
                    model_id = model_id_map.get(key)

                    if model_id is None:
                        skipped_no_model += 1
                        # 필요하면 경고 로그를 남길 수도 있다.
                        # print(f"[WARN] car_model에 없는 모델: {brand_name} / {model_name}")
                        continue

                    conn.execute(
                        text(
                            """
                            INSERT INTO model_monthly_sales (
                                model_id,
                                month,
                                sales_units,
                                market_total_units,
                                adoption_rate,
                                source
                            )
                            VALUES (
                                :model_id,
                                :month,
                                :sales_units,
                                :market_total_units,
                                :adoption_rate,
                                :source
                            )
                            ON DUPLICATE KEY UPDATE
                                sales_units = VALUES(sales_units),
                                market_total_units = VALUES(market_total_units),
                                adoption_rate = VALUES(adoption_rate),
                                source = VALUES(source)
                            """
                        ),
                        {
                            "model_id": model_id,
                            "month": month_date,
                            "sales_units": sales_units,
                            "market_total_units": None,
                            "adoption_rate": None,
                            "source": "DANAWA",
                        },
                    )
                    inserted_rows += 1

        print(f"[DONE] 총 행 수: {total_rows}")
        print(f"[DONE] 삽입/업데이트된 행 수: {inserted_rows}")
        print(f"[DONE] car_model에 매칭되지 않아 스킵된 행 수: {skipped_no_model}")


def main():
    print(f"[INFO] DANAWA_BASE: {DANAWA_BASE}")
    load_sales()


if __name__ == "__main__":
    main()
