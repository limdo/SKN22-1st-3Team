# src/etl/sales/run_danawa_model_crawl.py

from __future__ import annotations

import argparse
from pathlib import Path
from typing import List

from src.etl.sales.danawa_selenium import get_driver
from src.etl.sales.danawa_scraper import (
    scrape_month_for_brand,
    save_sales_csv,
    save_meta_csv,
    Brand,
)
from src.etl.sales.danawa_normalizer import normalize_folder


BASE_DIR = Path(__file__).resolve().parents[3]  # 프로젝트 루트


def build_month_list(year: int, start_month: int, end_month: int) -> List[str]:
    months: List[str] = []
    for m in range(start_month, end_month + 1):
        months.append(f"{year}-{m:02d}-00")
    return months


def run_crawl(
    run_id: str,
    year: int,
    start_month: int,
    end_month: int,
    brands: List[Brand],
    headless: bool = True,
) -> None:
    months = build_month_list(year, start_month, end_month)
    base_raw = BASE_DIR / "data" / "raw" / "danawa" / run_id

    driver = get_driver(headless=headless)
    try:
        for month in months:
            for brand in brands:
                rows = scrape_month_for_brand(driver, brand=brand, month=month)

                if not rows:
                    continue

                brand_dir = base_raw / brand
                brand_dir.mkdir(parents=True, exist_ok=True)

                # raw 판매량 CSV: 기존 팀원 명명 규칙 유지
                sales_filename = f"{brand}_model_sales_{month.replace('-', '_')}.csv"
                sales_path = brand_dir / sales_filename
                save_sales_csv(rows, sales_path)

                # 메타 CSV: 모델 상세 URL / 이미지 URL
                meta_filename = f"{brand}_model_meta_{month.replace('-', '_')}.csv"
                meta_path = brand_dir / meta_filename
                save_meta_csv(rows, meta_path)

                # 바로 이 폴더에 대해 normalized CSV 생성
                normalize_folder(brand_dir)

    finally:
        driver.quit()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True, help="실행 식별자 (예: 25_11_16)")
    parser.add_argument("--year", type=int, required=True, help="수집할 연도 (예: 2023)")
    parser.add_argument("--start-month", type=int, default=1)
    parser.add_argument("--end-month", type=int, default=12)
    parser.add_argument(
        "--brands",
        nargs="+",
        default=["hyundai", "kia"],
        help="대상 브랜드 목록 (예: hyundai kia)",
    )
    parser.add_argument(
        "--no-headless",
        action="store_true",
        help="지정하면 브라우저 창을 실제로 띄움",
    )

    args = parser.parse_args()

    brands: List[Brand] = [b for b in args.brands]  # 간단 캐스팅

    run_crawl(
        run_id=args.run_id,
        year=args.year,
        start_month=args.start_month,
        end_month=args.end_month,
        brands=brands,
        headless=not args.no_headless,
    )


if __name__ == "__main__":
    main()