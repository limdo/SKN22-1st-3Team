# src/etl/sales/load_danawa_meta_to_db.py

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse, parse_qs

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
class MetaRow:
    brand_code: str
    month: str
    rank: str
    model_name: str
    detail_url: Optional[str]
    image_url: Optional[str]


def extract_model_id_from_url(url: str | None) -> Optional[int]:
    """
    예: https://auto.danawa.com/auto/?Work=model&Model=33191 → 33191
    """
    if not url:
        return None

    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    vals = qs.get("Model") or qs.get("model")
    if not vals:
        return None
    try:
        return int(vals[0])
    except ValueError:
        return None


def load_meta_csv(path: Path, brand_code_from_dir: str) -> List[MetaRow]:
    rows: List[MetaRow] = []

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            brand_code = (r.get("brand") or "").strip() or brand_code_from_dir
            month = (r.get("month") or "").strip()
            rank = (r.get("rank") or "").strip()
            model_name = (r.get("model_name") or "").strip()
            detail_url = (r.get("detail_url") or "").strip()
            image_url = (r.get("image_url") or "").strip()

            if not model_name:
                continue

            rows.append(
                MetaRow(
                    brand_code=brand_code.lower(),
                    month=month,
                    rank=rank,
                    model_name=model_name,
                    detail_url=detail_url or None,
                    image_url=image_url or None,
                )
            )

    return rows


def process_meta_for_brand(
    conn,
    run_id: str,
    brand_code: str,
    stats: Dict[str, int],
) -> None:
    """
    특정 run_id / brand 에 대해:
      data/raw/danawa/<run_id>/<brand>/*_model_meta_*.csv 를 모두 처리
    """
    brand_dir = DANAWA_RAW_BASE / run_id / brand_code
    if not brand_dir.exists():
        print(f"[WARN] 브랜드 디렉토리 없음: {brand_dir}")
        return

    print(
        f"\n[INFO] 메타 처리 시작: run_id={run_id}, brand={brand_code}, dir={brand_dir}"
    )

    brand_name_kr = BRAND_KR_MAP.get(brand_code.lower())
    if not brand_name_kr:
        print(f"[WARN] BRAND_KR_MAP에 없는 브랜드 코드: {brand_code}")
        return

    meta_files = sorted(brand_dir.glob("*_model_meta_*.csv"))
    if not meta_files:
        print(f"[WARN] 메타 CSV 없음: {brand_dir}")
        return

    for path in meta_files:
        print(f"[INFO] 메타 파일 처리: {path}")
        meta_rows = load_meta_csv(path, brand_code_from_dir=brand_code)

        for mr in meta_rows:
            stats["total_rows"] += 1

            db_brand_name = BRAND_KR_MAP.get(mr.brand_code, brand_name_kr)

            # car_model 찾기
            row = conn.execute(
                text(
                    """
                    SELECT model_id, brand_name, model_name_kr,
                           danawa_model_id, danawa_model_url
                    FROM car_model
                    WHERE brand_name = :brand_name
                      AND model_name_kr = :model_name_kr
                    """
                ),
                {
                    "brand_name": db_brand_name,
                    "model_name_kr": mr.model_name,
                },
            ).fetchone()

            if not row:
                stats["no_model_match"] += 1
                # print(f"[WARN] car_model 매칭 실패: brand={db_brand_name}, model_name={mr.model_name}")
                continue

            model_id = row.model_id

            # car_model 업데이트: danawa_model_id / danawa_model_url
            danawa_model_id = extract_model_id_from_url(mr.detail_url)
            danawa_model_url = mr.detail_url

            # 1) danawa_model_id 충돌 체크
            danawa_model_id_for_update = None
            if danawa_model_id is not None:
                conflict = conn.execute(
                    text(
                        """
                        SELECT model_id
                        FROM car_model
                        WHERE danawa_model_id = :danawa_model_id
                        LIMIT 1
                        """
                    ),
                    {"danawa_model_id": danawa_model_id},
                ).fetchone()

                if conflict and conflict.model_id != model_id:
                    # 이미 다른 모델이 이 danawa_model_id 를 사용하고 있음 → 충돌 발생
                    stats.setdefault("danawa_id_conflict", 0)
                    stats["danawa_id_conflict"] += 1
                    # 이 row에서는 danawa_model_id 업데이트를 건너뛴다.
                    danawa_model_id_for_update = None
                    # URL만 업데이트할지, 아예 스킵할지 정책 선택 가능
                    # 여기선 URL은 업데이트 허용
                else:
                    danawa_model_id_for_update = danawa_model_id
            # 2) UPDATE 수행
            conn.execute(
                text(
                    """
                    UPDATE car_model
                    SET
                        danawa_model_id = CASE
                            WHEN :danawa_model_id IS NOT NULL THEN :danawa_model_id
                            ELSE danawa_model_id
                        END,
                        danawa_model_url = CASE
                            WHEN :danawa_model_url IS NOT NULL AND :danawa_model_url <> '' THEN :danawa_model_url
                            ELSE danawa_model_url
                        END
                    WHERE model_id = :model_id
                    """
                ),
                {
                    "danawa_model_id": danawa_model_id_for_update,
                    "danawa_model_url": danawa_model_url,
                    "model_id": model_id,
                },
            )
            stats["car_model_updated"] += 1

            # car_model_image 삽입
            if mr.image_url:
                exists = conn.execute(
                    text(
                        """
                        SELECT image_id
                        FROM car_model_image
                        WHERE model_id = :model_id
                          AND image_url = :image_url
                        LIMIT 1
                        """
                    ),
                    {
                        "model_id": model_id,
                        "image_url": mr.image_url,
                    },
                ).fetchone()

                if not exists:
                    conn.execute(
                        text(
                            """
                            INSERT INTO car_model_image (
                                model_id,
                                image_url,
                                local_path,
                                content_type,
                                image_binary,
                                is_primary,
                                created_at
                            )
                            VALUES (
                                :model_id,
                                :image_url,
                                NULL,
                                NULL,
                                NULL,
                                1,
                                NOW()
                            )
                            """
                        ),
                        {
                            "model_id": model_id,
                            "image_url": mr.image_url,
                        },
                    )
                    stats["image_inserted"] += 1
                else:
                    stats["image_skipped_duplicate"] += 1


def run_loader(run_id: str, brands: List[str]) -> None:
    engine = get_engine(echo=False)

    stats = {
        "total_rows": 0,
        "no_model_match": 0,
        "car_model_updated": 0,
        "image_inserted": 0,
        "image_skipped_duplicate": 0,
        "danawa_id_conflict": 0,
    }

    with engine.begin() as conn:
        for brand in brands:
            process_meta_for_brand(conn, run_id=run_id, brand_code=brand, stats=stats)

    print("\n[SUMMARY] 다나와 메타 로더 결과")
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
        help="대상 브랜드 코드 목록 (예: hyundai kia)",
    )

    args = parser.parse_args()
    run_loader(run_id=args.run_id, brands=args.brands)


if __name__ == "__main__":
    main()
