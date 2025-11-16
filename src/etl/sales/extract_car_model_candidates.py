# src/etl/sales/extract_car_model_candidates.py

import csv
import re
from dataclasses import dataclass
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[3]  # 프로젝트 루트
DANAWA_BASE = BASE_DIR / "data" / "raw" / "danawa" / "25_11_14"
OUTPUT_PATH = BASE_DIR / "data" / "raw" / "car_model_candidates.csv"


@dataclass
class ModelStat:
    brand_name: str
    model_name_kr: str
    first_month: str | None  # 'YYYY-MM'
    last_month: str | None  # 'YYYY-MM'
    months: set
    total_sales: int

    def update(self, month: str, sales: int):
        if self.first_month is None or month < self.first_month:
            self.first_month = month
        if self.last_month is None or month > self.last_month:
            self.last_month = month
        self.months.add(month)
        self.total_sales += sales

    def to_row(self) -> dict:
        return {
            "brand_name": self.brand_name,
            "model_name_kr": self.model_name_kr,
            "first_month": self.first_month or "",
            "last_month": self.last_month or "",
            "months_count": len(self.months),
            "total_sales": self.total_sales,
        }


def parse_month_from_filename(filename: str) -> str:
    """
    예: hyundai_model_sales_2024_06_00_normalized.csv → '2024-06'
    """
    m = re.search(r"(\d{4})_(\d{2})_00", filename)
    if not m:
        raise ValueError(f"파일명에서 연월을 찾을 수 없음: {filename}")
    year, month = m.group(1), m.group(2)
    return f"{year}-{month}"


def iter_normalized_files():
    """
    현대/기아 normalized CSV 전체 경로 + 브랜드 이름을 yield
    """
    for brand_name, subdir in [("현대", "hyundai"), ("기아", "kia")]:
        brand_dir = DANAWA_BASE / subdir
        for path in sorted(brand_dir.glob("*_normalized.csv")):
            yield brand_name, path


def build_model_candidates() -> dict[tuple[str, str], ModelStat]:
    stats: dict[tuple[str, str], ModelStat] = {}

    for brand_name, path in iter_normalized_files():
        month = parse_month_from_filename(path.name)

        with path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            # 기대 컬럼: 순위,모델명,판매량,점유율,전월대비,전년대비
            for row in reader:
                model_name = row.get("모델명")
                sales_str = row.get("판매량")

                if not model_name:
                    continue

                try:
                    sales = int(sales_str.replace(",", "")) if sales_str else 0
                except ValueError:
                    sales = 0

                key = (brand_name, model_name)
                if key not in stats:
                    stats[key] = ModelStat(
                        brand_name=brand_name,
                        model_name_kr=model_name,
                        first_month=None,
                        last_month=None,
                        months=set(),
                        total_sales=0,
                    )

                stats[key].update(month, sales)

    return stats


def save_candidates_to_csv(stats: dict[tuple[str, str], ModelStat]):
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "brand_name",
        "model_name_kr",
        "first_month",
        "last_month",
        "months_count",
        "total_sales",
    ]

    with OUTPUT_PATH.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for key in sorted(stats.keys()):
            writer.writerow(stats[key].to_row())


def main():
    stats = build_model_candidates()
    save_candidates_to_csv(stats)
    print(f"총 모델 수: {len(stats)}개")
    print(f"→ {OUTPUT_PATH} 에 후보 리스트 저장 완료")


if __name__ == "__main__":
    main()
