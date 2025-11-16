# src/etl/sales/danawa_normalizer.py

from __future__ import annotations

import csv
import os
import re
from pathlib import Path
from typing import List, Optional


def parse_int_from_str(s: str) -> Optional[int]:
    """
    '12,345대' 같은 문자열에서 숫자만 추출해 int로 변환.
    숫자가 하나도 없으면 None.
    """
    if s is None:
        return None
    s = s.strip()
    if not s:
        return None
    digits = re.findall(r"\d+", s.replace(",", ""))
    if not digits:
        return None
    return int("".join(digits))


def parse_change_field(s: str) -> Optional[int]:
    """
    '9118 697▲' 같은 전월/전년대비 문자열에서
    '증감량'만 정규화해서 정수로 반환한다.

    예)
      '9118 697▲'  -> +697
      '6578 351▼'  -> -351
      '0 9815▲'    -> +9815
      ''           -> None
    """
    if not s:
        return None

    s = s.strip()
    if not s or s == "-":
        return None

    parts = s.split()
    base_part = None
    diff_part = None

    if len(parts) == 1:
        # '697▲' 처럼 하나만 온 경우 → 이걸 diff로 간주
        diff_part = parts[0]
    else:
        # '9118 697▲' → base='9118', diff='697▲'
        base_part, diff_part = parts[0], parts[1]

    # diff 파트에서 부호/숫자 추출
    sign = 1
    if "▼" in diff_part:
        sign = -1
    elif "▲" in diff_part:
        sign = 1

    digits = re.findall(r"\d+", diff_part.replace(",", ""))
    if not digits:
        return None

    val = int("".join(digits))
    return sign * val


def normalize_row(raw_row: List[str]) -> Optional[List[str]]:
    """
    raw CSV 한 줄을 완전 정규화된 한 줄로 변환.

    raw 형식 가정:
      [순위, (옵션)빈칸, 모델명, 판매량, 점유율, 전월대비, 전년대비]
    또는
      [순위, 모델명, 판매량, 점유율, 전월대비, 전년대비]
    """
    if not raw_row:
        return None

    # BOM 제거 + 공백 제거
    raw_row = [(c or "").strip() for c in raw_row]

    # 컬럼 개수에 따라 매핑
    if len(raw_row) >= 7:
        # 크롤러에서 바로 저장한 형태 (중간에 빈 칼럼 하나 있음)
        rank = raw_row[0]
        model_name = raw_row[2]
        sales_str = raw_row[3]
        share_str = raw_row[4]
        mom_str = raw_row[5]
        yoy_str = raw_row[6]
    elif len(raw_row) >= 6:
        # 기존 팀원이 만든 nomalized/normalized 형태
        rank = raw_row[0]
        model_name = raw_row[1]
        sales_str = raw_row[2]
        share_str = raw_row[3]
        mom_str = raw_row[4]
        yoy_str = raw_row[5]
    else:
        # 우리가 예상하는 최소 컬럼보다 적으면 스킵
        return None

    if not rank or not model_name:
        return None

    # 판매량 정수화
    sales_units = parse_int_from_str(sales_str)
    if sales_units is None:
        return None

    # 점유율: 숫자(실수)만 남기기 (예: '17.7%', '17.7 %' → '17.7')
    share_ratio = ""
    if share_str:
        m = re.search(r"-?\d+(?:\.\d+)?", share_str.replace(",", ""))
        if m:
            share_ratio = m.group(0)

    # 전월대비/전년대비: 증감량만 정수로 파싱
    mom_diff = parse_change_field(mom_str)
    yoy_diff = parse_change_field(yoy_str)

    # 문자열로 변환 (없으면 빈 문자열)
    mom_diff_str = "" if mom_diff is None else str(mom_diff)
    yoy_diff_str = "" if yoy_diff is None else str(yoy_diff)

    return [
        rank,
        model_name,
        str(sales_units),
        share_ratio,
        mom_diff_str,
        yoy_diff_str,
    ]


def normalize_folder(folder_path: Path) -> None:
    """
    한 브랜드 폴더(hyundai/ 또는 kia/) 안에 있는
    판매량 CSV(원본 또는 nomalized/normalized 둘 다)를 읽어서
    *_normalized.csv로 다시 저장한다.

    - *_normalized.csv 파일은 이 함수로 "다시" 덮어써도 된다.
    - 메타 CSV(*_meta_*.csv)는 건너뛴다.
    """
    print(f"\n[INFO] 폴더 정규화 시작: {folder_path}")

    for filename in os.listdir(folder_path):
        if not filename.endswith(".csv"):
            continue
        if "_meta_" in filename:
            # 메타 정보 CSV는 정규화 대상이 아님
            continue

        input_path = folder_path / filename

        # 출력 파일명: *_normalized.csv 로 통일
        if filename.endswith("_normalized.csv"):
            output_path = input_path  # 덮어쓰기
        elif filename.endswith("_nomalized.csv"):
            # 팀원이 만든 오타 버전 → 이름 통일하면서 새 파일 생성
            output_path = folder_path / filename.replace(
                "_nomalized.csv", "_normalized.csv"
            )
        else:
            output_path = folder_path / filename.replace(".csv", "_normalized.csv")

        print(f"[INFO] 파일 처리: {input_path} -> {output_path}")

        normalized_rows: List[List[str]] = []

        with input_path.open("r", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            try:
                header = next(reader)
            except StopIteration:
                continue

            for row in reader:
                if not row:
                    continue
                norm = normalize_row(row)
                if norm is None:
                    continue
                normalized_rows.append(norm)

        if not normalized_rows:
            print(f"[WARN] 정규화 결과가 비어 있음: {input_path}")
            continue

        with output_path.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            # 최종 정규화 헤더
            writer.writerow(
                ["순위", "모델명", "판매량", "점유율", "전월대비", "전년대비"]
            )
            writer.writerows(normalized_rows)

        print(f"[INFO] 저장 완료: {output_path}")


# if __name__ == "__main__":
#     base_dir = Path(__file__).resolve().parents[3]
#     # folder = base_dir / "data" / "raw" / "danawa" / "25_11_14" / "hyundai"
#     # normalize_folder(folder)

#     folder = base_dir / "data" / "raw" / "danawa" / "25_11_14" / "kia"
#     normalize_folder(folder)
