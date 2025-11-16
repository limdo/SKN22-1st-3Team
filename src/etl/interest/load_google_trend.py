# src/etl/interest/load_google_trend.py

from __future__ import annotations

import argparse
import csv
from pathlib import Path

from sqlalchemy import text

from src.db.connection import get_engine


BASE_DIR = Path(__file__).resolve().parents[3]
GOOGLE_DIR = BASE_DIR / "data" / "raw" / "google"


def load_google_trend(run_id: str) -> None:
    """
    정규화된 구글 트렌드 CSV를 읽어서
    model_monthly_interest.google_trend_index 를 upsert.
    """
    csv_path = GOOGLE_DIR / run_id / f"google_trend_{run_id}_normalized.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"정규화된 구글 트렌드 CSV가 없습니다: {csv_path}")

    print(f"[INFO] 구글 트렌드 로딩 시작: {csv_path}")

    engine = get_engine(echo=False)

    sql = text(
        """
        INSERT INTO model_monthly_interest (
            model_id,
            month,
            naver_search_index,
            google_trend_index,
            danawa_pop_rank,
            danawa_pop_rank_size,
            created_at
        )
        VALUES (
            :model_id,
            :month,
            NULL,
            :google_trend_index,
            NULL,
            NULL,
            NOW()
        )
        ON DUPLICATE KEY UPDATE
            google_trend_index = VALUES(google_trend_index)
        """
    )

    rows = 0

    with engine.begin() as conn, csv_path.open("r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                model_id = int(row["model_id"])
                month = row["month"]
                google_trend_index = int(row["google_trend_index"])
            except (KeyError, ValueError) as e:
                print(f"[WARN] 행 스킵: row={row}, error={e}")
                continue

            conn.execute(
                sql,
                {
                    "model_id": model_id,
                    "month": month,
                    "google_trend_index": google_trend_index,
                },
            )
            rows += 1

    print(f"[INFO] model_monthly_interest.google_trend_index upsert 완료 (rows={rows})")


def main():
    parser = argparse.ArgumentParser(
        description="구글 트렌드 → model_monthly_interest 로더"
    )
    parser.add_argument("--run-id", required=True, help="실행 ID (예: 25_11_16)")
    args = parser.parse_args()

    load_google_trend(run_id=args.run_id)


if __name__ == "__main__":
    main()
