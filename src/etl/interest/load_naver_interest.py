# src/etl/interest/load_naver_interest.py

from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Tuple, List

from sqlalchemy import text

from src.db.connection import get_engine


BASE_DIR = Path(__file__).resolve().parents[3]  # 프로젝트 루트
NAVER_RAW_BASE = BASE_DIR / "data" / "raw" / "naver"


@dataclass
class InterestPoint:
    model_id: int
    month: str  # 'YYYY-MM-01'
    naver_index: float


def month_from_date(date_str: str) -> str:
    """
    'YYYY-MM-DD' 형식 문자열에서 'YYYY-MM-01' 로 통일.
    (네이버 timeUnit=month 이면 원래 1일이라 그냥 방어 차원용.)
    """
    if len(date_str) >= 7:
        return date_str[:7] + "-01"
    raise ValueError(f"예상치 못한 날짜 형식: {date_str}")


def load_raw_csv(run_id: str) -> List[InterestPoint]:
    """
    data/raw/naver/<run_id>/naver_trend_<run_id>.csv 읽어서
    (model_id, month) 기준 InterestPoint 리스트로 반환.
    """
    csv_path = NAVER_RAW_BASE / run_id / f"naver_trend_{run_id}.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"네이버 raw CSV를 찾을 수 없습니다: {csv_path}")

    print(f"[INFO] raw CSV 로딩: {csv_path}")

    # (model_id, month) -> [ratio, ratio, ...]
    bucket: Dict[Tuple[int, str], List[float]] = defaultdict(list)

    with csv_path.open("r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                model_id = int(row["model_id"])
            except (KeyError, ValueError):
                continue

            date_str = (row.get("date") or "").strip()
            ratio_str = (row.get("ratio") or "").strip()

            if not date_str or not ratio_str:
                continue

            month = month_from_date(date_str)

            try:
                ratio = float(ratio_str)
            except ValueError:
                continue

            bucket[(model_id, month)].append(ratio)

    points: List[InterestPoint] = []

    for (model_id, month), ratios in bucket.items():
        if not ratios:
            continue
        avg_ratio = sum(ratios) / len(ratios)
        points.append(
            InterestPoint(
                model_id=model_id,
                month=month,
                naver_index=avg_ratio,
            )
        )

    print(f"[INFO] 집계된 (model_id, month) 개수: {len(points)}")
    return points


def upsert_naver_interest(points: List[InterestPoint]) -> None:
    """
    model_monthly_interest 테이블에 naver_index upsert.
    google_index, danawa_popularity 는 일단 NULL로 둔다.
    """
    if not points:
        print("[WARN] 적재할 데이터가 없습니다.")
        return

    engine = get_engine(echo=False)

    sql = text(
        """
        INSERT INTO model_monthly_interest (
            model_id,
            month,
            naver_index,
            google_index,
            danawa_popularity,
            created_at
        )
        VALUES (
            :model_id,
            :month,
            :naver_index,
            NULL,
            NULL,
            NOW()
        )
        ON DUPLICATE KEY UPDATE
            naver_index = VALUES(naver_index)
        """
    )

    with engine.begin() as conn:
        for p in points:
            conn.execute(
                sql,
                {
                    "model_id": p.model_id,
                    "month": p.month,
                    "naver_index": p.naver_index,
                },
            )

    print(f"[INFO] model_monthly_interest upsert 완료 (rows={len(points)})")


def run_loader(run_id: str) -> None:
    points = load_raw_csv(run_id)
    upsert_naver_interest(points)


def main():
    parser = argparse.ArgumentParser(
        description="네이버 관심도 → model_monthly_interest 로더"
    )
    parser.add_argument("--run-id", required=True, help="수집 실행 ID (예: 25_11_16)")

    args = parser.parse_args()
    run_loader(run_id=args.run_id)


if __name__ == "__main__":
    main()
