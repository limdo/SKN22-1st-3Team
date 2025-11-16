# src/etl/sales/load_danawa_sales_to_db.py

from __future__ import annotations

import argparse
import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from sqlalchemy import text

from src.db.connection import get_engine


BASE_DIR = Path(__file__).resolve().parents[3]  # 프로젝트 루트
DANAWA_RAW_BASE = BASE_DIR / "data" / "raw" / "danawa"

# 크롤러에서 쓰는 브랜드 코드 → DB의 brand_name 매핑
BRAND_KR_MAP: Dict[str, str] = {
    "hyundai": "현대",
    "kia": "기아",
}


@dataclass
class SalesRow:
    brand_code: str
    month: str  # "2023-01-01" 같은 DATE 문자열
    rank: int
    model_name: str
    sales_units: int
    share_ratio: Optional[float]  # 0.1234 이런 형태 (점유율 % / 100)


def parse_int_from_str(s: str) -> Optional[int]:
    """
    '12,345대' 같은 문자열에서 숫자만 추출해 int로 변환.
    숫자가 하나도 없으면 None.
    """
    if s is None:
        return None
    digits = re.findall(r"\d+", s.replace(",", ""))
    if not digits:
        return None
    return int("".join(digits))


def parse_share_ratio(s: str) -> Optional[float]:
    """
    '12.3%', '12.3 %', '12.3' 같은 문자열에서
    첫 번째 실수 값을 찾아서 100으로 나눈 비율(0.xxxx)로 반환.
    """
    if not s:
        return None

    s = s.replace(",", "")
    m = re.search(r"(-?\d+(?:\.\d+)?)", s)
    if not m:
        return None
    try:
        val = float(m.group(1))
    except ValueError:
        return None
    return val / 100.0


def extract_month_date_from_filename(stem: str) -> str:
    """
    예: 'kia_model_sales_2023_01_00_normalized' 에서
        '2023-01-01' 로 변환.
    """
    m = re.search(r"(\d{4})_(\d{2})_00", stem)
    if not m:
        raise ValueError(f"파일명에서 월 정보를 찾을 수 없음: {stem}")
    year, month = m.group(1), m.group(2)
    return f"{year}-{month}-01"


def load_normalized_sales_csv(path: Path, brand_code_from_dir: str) -> List[SalesRow]:
    """
    *_model_sales_*_normalized.csv 파일 하나를 읽어서 SalesRow 리스트로 반환.
    정규화된 CSV 헤더: 순위,모델명,판매량,점유율,전월대비,전년대비
    """
    month_date = extract_month_date_from_filename(path.stem)
    rows: List[SalesRow] = []

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rank_str = (r.get("순위") or "").strip()
            model_name = (r.get("모델명") or "").strip()
            sales_str = (r.get("판매량") or "").strip()
            share_str = (r.get("점유율") or "").strip()

            if not model_name:
                continue

            rank = parse_int_from_str(rank_str) or 0
            sales_units = parse_int_from_str(sales_str)
            if sales_units is None:
                # 판매량이 숫자로 파싱 안되면 스킵
                continue

            share_ratio = parse_share_ratio(share_str)

            rows.append(
                SalesRow(
                    brand_code=brand_code_from_dir.lower(),
                    month=month_date,
                    rank=rank,
                    model_name=model_name,
                    sales_units=sales_units,
                    share_ratio=share_ratio,
                )
            )

    return rows


def process_sales_for_brand(
    conn, run_id: str, brand_code: str, stats: Dict[str, int]
) -> None:
    """
    특정 run_id / brand 에 대해:
      data/raw/danawa/<run_id>/<brand>/*_model_sales_*_normalized.csv 를 모두 처리
    """
    brand_dir = DANAWA_RAW_BASE / run_id / brand_code
    if not brand_dir.exists():
        print(f"[WARN] 브랜드 디렉토리 없음: {brand_dir}")
        return

    print(
        f"\n[INFO] 판매량 로더 시작: run_id={run_id}, brand={brand_code}, dir={brand_dir}"
    )

    brand_name_kr = BRAND_KR_MAP.get(brand_code.lower())
    if not brand_name_kr:
        print(f"[WARN] BRAND_KR_MAP에 없는 브랜드 코드: {brand_code}")
        return

    sales_files = sorted(brand_dir.glob("*_model_sales_*_normalized.csv"))
    if not sales_files:
        print(f"[WARN] 정규화된 판매량 CSV 없음: {brand_dir}")
        return

    for path in sales_files:
        print(f"[INFO] 판매량 파일 처리: {path}")
        sales_rows = load_normalized_sales_csv(path, brand_code_from_dir=brand_code)
        if not sales_rows:
            continue

        # 같은 파일(=같은 month, 같은 brand) 내에서 total_units 계산
        total_units_by_month: Dict[str, int] = {}
        for sr in sales_rows:
            total_units_by_month.setdefault(sr.month, 0)
            total_units_by_month[sr.month] += sr.sales_units

        for sr in sales_rows:
            stats["total_rows"] += 1

            db_brand_name = BRAND_KR_MAP.get(sr.brand_code, brand_name_kr)

            # car_model 매칭
            row = conn.execute(
                text(
                    """
                    SELECT model_id
                    FROM car_model
                    WHERE brand_name = :brand_name
                      AND model_name_kr = :model_name_kr
                    """
                ),
                {
                    "brand_name": db_brand_name,
                    "model_name_kr": sr.model_name,
                },
            ).fetchone()

            if not row:
                stats["no_model_match"] += 1
                # print(f"[WARN] car_model 매칭 실패: brand={db_brand_name}, model_name={sr.model_name}")
                continue

            model_id = row.model_id
            market_total_units = total_units_by_month.get(sr.month, 0)

            # adoption_rate 계산:
            # 1순위: 점유율(share_ratio) 값이 있으면 그대로 사용
            # 2순위: 없으면 sales_units / market_total_units
            if sr.share_ratio is not None:
                adoption_rate = sr.share_ratio
            else:
                adoption_rate = (
                    sr.sales_units / market_total_units if market_total_units else None
                )

            conn.execute(
                text(
                    """
                    INSERT INTO model_monthly_sales (
                        model_id,
                        month,
                        sales_units,
                        market_total_units,
                        adoption_rate,
                        source,
                        created_at
                    )
                    VALUES (
                        :model_id,
                        :month,
                        :sales_units,
                        :market_total_units,
                        :adoption_rate,
                        'DANAWA',
                        NOW()
                    )
                    ON DUPLICATE KEY UPDATE
                        sales_units        = VALUES(sales_units),
                        market_total_units = VALUES(market_total_units),
                        adoption_rate      = VALUES(adoption_rate),
                        source             = VALUES(source)
                    """
                ),
                {
                    "model_id": model_id,
                    "month": sr.month,
                    "sales_units": sr.sales_units,
                    "market_total_units": market_total_units or None,
                    "adoption_rate": adoption_rate,
                },
            )
            stats["insert_or_update"] += 1


def run_loader(run_id: str, brands: List[str]) -> None:
    engine = get_engine(echo=False)

    stats: Dict[str, int] = {
        "total_rows": 0,
        "no_model_match": 0,
        "insert_or_update": 0,
    }

    with engine.begin() as conn:
        for brand in brands:
            process_sales_for_brand(conn, run_id=run_id, brand_code=brand, stats=stats)

    print("\n[SUMMARY] 다나와 판매량 로더 결과")
    for k, v in stats.items():
        print(f"  {k}: {v}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--run-id", required=True, help="다나와 수집 실행 ID (예: test_run, 25_11_16)"
    )
    parser.add_argument(
        "--brands",
        nargs="+",
        default=["hyundai", "kia"],
        help="대상 브랜드 코드 (예: hyundai kia)",
    )
    args = parser.parse_args()

    run_loader(run_id=args.run_id, brands=args.brands)


if __name__ == "__main__":
    main()
