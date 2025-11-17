# src/etl/blog/generate_wordcloud.py

from __future__ import annotations

import argparse
import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional

from wordcloud import WordCloud
import matplotlib.pyplot as plt
from matplotlib import font_manager as fm

from sqlalchemy import text

from src.db.connection import get_engine


BASE_DIR = Path(__file__).resolve().parents[3]

DEFAULT_FONT_CANDIDATES = [
    "NanumGothic",
    "NanumSquare",
    "AppleGothic",
    "Malgun Gothic",
    "Noto Sans CJK KR",
    "Arial Unicode MS",
]


def resolve_font_path(font_path: Optional[str]) -> Optional[str]:
    """
    사용자가 지정한 폰트 경로가 있으면 그대로 사용한다.
    없으면 시스템에 설치된 한글 폰트 후보를 탐색해 반환한다.
    """
    if font_path:
        candidate = Path(font_path).expanduser()
        if candidate.exists():
            return str(candidate)
        print(f"[WARN] 지정한 폰트 경로를 찾을 수 없습니다: {candidate}")

    for font_name in DEFAULT_FONT_CANDIDATES:
        try:
            resolved = fm.findfont(font_name, fallback_to_default=False)
            if resolved and Path(resolved).exists():
                print(f"[INFO] '{font_name}' 폰트를 사용합니다: {resolved}")
                return resolved
        except Exception:
            continue

    # 최종적으로 matplotlib 기본 폰트 경로라도 사용
    fallback = fm.findfont(fm.FontProperties())
    print(f"[WARN] 한글 폰트를 찾지 못해 기본 폰트를 사용합니다: {fallback}")
    return fallback

def parse_month_arg(month_str: str | None) -> datetime.date:
    """--month 가 없으면 오늘 기준으로 YYYY-MM-01, 있으면 그 값으로."""
    if month_str is None:
        today = datetime.date.today()
        return today.replace(day=1)
    # "YYYY-MM" 또는 "YYYY-MM-01" 둘 다 허용
    if len(month_str) == 7:
        month_str = month_str + "-01"
    return datetime.datetime.strptime(month_str, "%Y-%m-%d").date()


def load_token_counts_by_model(
    month: datetime.date,
    limit_models: int | None = None,
) -> Dict[int, Dict[str, int]]:
    """
    blog_token_monthly 에서 해당 month 의 토큰 빈도를
    model_id 별로 묶어서 반환.

    return:
        {
            model_id: { token: total_count, ... },
            ...
        }
    """
    engine = get_engine(echo=False)
    sql = text(
        """
        SELECT
            bt.model_id,
            bt.token,
            bt.total_count
        FROM blog_token_monthly bt
        WHERE bt.month = :month
        ORDER BY bt.model_id, bt.token_rank
        """
    )

    result: Dict[int, Dict[str, int]] = {}
    with engine.connect() as conn:
        rows = conn.execute(sql, {"month": month}).mappings().all()

    for row in rows:
        mid = row["model_id"]
        token = row["token"]
        count = row["total_count"]
        if mid not in result:
            result[mid] = {}
        # 중복 토큰이 있을 일은 없지만, 혹시 모르니 누적
        result[mid][token] = result[mid].get(token, 0) + count

    # limit_models 가 있으면 앞에서 일부만 자르기
    if limit_models is not None:
        limited: Dict[int, Dict[str, int]] = {}
        for i, (mid, tokens) in enumerate(result.items()):
            if i >= limit_models:
                break
            limited[mid] = tokens
        return limited

    return result


def load_model_names(model_ids: List[int]) -> Dict[int, Tuple[str, str]]:
    """
    car_model 에서 brand_name, model_name_kr 가져오기.

    return:
        { model_id: (brand_name, model_name_kr), ... }
    """
    if not model_ids:
        return {}

    engine = get_engine(echo=False)
    sql = text(
        """
        SELECT model_id, brand_name, model_name_kr
        FROM car_model
        WHERE model_id IN :ids
        """
    )

    with engine.connect() as conn:
        rows = conn.execute(sql, {"ids": tuple(model_ids)}).mappings().all()

    result: Dict[int, Tuple[str, str]] = {}
    for row in rows:
        result[row["model_id"]] = (row["brand_name"], row["model_name_kr"])
    return result


def ensure_output_dir(month: datetime.date) -> Path:
    out_dir = BASE_DIR / "data" / "processed" / "blog_wc" / f"{month:%Y%m}"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def build_filename(
    out_dir: Path,
    model_id: int,
    brand_name: str | None,
    model_name_kr: str | None,
) -> Path:
    """
    파일명: model_{id}_{brand}_{model}.png
    한글/공백은 대충 '_' 로 정리.
    """

    def _sanitize(s: str) -> str:
        return (
            s.replace(" ", "_")
            .replace("/", "_")
            .replace("\\", "_")
            .replace("(", "")
            .replace(")", "")
        )

    brand_part = _sanitize(brand_name) if brand_name else f"model_{model_id}"
    model_part = _sanitize(model_name_kr) if model_name_kr else ""
    name = f"model_{model_id}_{brand_part}_{model_part}.png"
    return out_dir / name


def generate_wordcloud_image(
    tokens: Dict[str, int],
    output_path: Path,
    font_path: str | None,
    width: int = 800,
    height: int = 600,
    max_words: int = 100,
) -> None:
    """
    주어진 토큰 빈도로 워드클라우드 이미지 생성 후 파일 저장.
    """
    if not tokens:
        return

    resolved_font = resolve_font_path(font_path)

    wc = WordCloud(
        font_path=resolved_font,
        width=width,
        height=height,
        background_color="white",
        max_words=max_words,
    )

    wc.generate_from_frequencies(tokens)

    # matplotlib 로 저장 (한 번도 plt.show() 안 쓰고 바로 저장)
    plt.figure(figsize=(width / 100, height / 100))
    plt.imshow(wc, interpolation="bilinear")
    plt.axis("off")
    plt.tight_layout(pad=0)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, bbox_inches="tight", pad_inches=0)
    plt.close()


def upsert_blog_wordcloud(
    model_id: int,
    month: datetime.date,
    image_path: str,
) -> None:
    """
    blog_wordcloud 에 (model_id, month) 기준 upsert.
    image_path 는 프로젝트 루트 기준 상대 경로로 넣는다.
    """
    engine = get_engine(echo=False)
    sql = text(
        """
        INSERT INTO blog_wordcloud (
            model_id,
            month,
            image_path,
            generated_at
        )
        VALUES (
            :model_id,
            :month,
            :image_path,
            NOW()
        )
        ON DUPLICATE KEY UPDATE
            image_path  = VALUES(image_path),
            generated_at = VALUES(generated_at)
        """
    )

    with engine.begin() as conn:
        conn.execute(
            sql,
            {
                "model_id": model_id,
                "month": month,
                "image_path": image_path,
            },
        )


def main():
    parser = argparse.ArgumentParser(
        description="blog_token_monthly 기반 워드클라우드 이미지 생성 ETL"
    )
    parser.add_argument("--run-id", required=True, help="실행 ID (로그용)")
    parser.add_argument(
        "--month",
        type=str,
        default=None,
        help="기준 월 (YYYY-MM 또는 YYYY-MM-DD, default: 오늘 기준 월)",
    )
    parser.add_argument(
        "--limit-models",
        type=int,
        default=None,
        help="테스트용: 앞에서 N개 모델만 처리",
    )
    parser.add_argument(
        "--font-path",
        type=str,
        default=None,
        help="워드클라우드 폰트 경로 (한글 폰트 권장)",
    )
    parser.add_argument(
        "--width",
        type=int,
        default=800,
        help="이미지 가로 크기(px)",
    )
    parser.add_argument(
        "--height",
        type=int,
        default=600,
        help="이미지 세로 크기(px)",
    )
    parser.add_argument(
        "--max-words",
        type=int,
        default=100,
        help="워드클라우드에 사용할 최대 단어 수",
    )
    args = parser.parse_args()

    month = parse_month_arg(args.month)
    print(f"[INFO] 워드클라우드 생성 기준 월 = {month}")

    token_by_model = load_token_counts_by_model(month, limit_models=args.limit_models)
    if not token_by_model:
        print("[WARN] 해당 월에 blog_token_monthly 데이터가 없습니다.")
        return

    model_ids = list(token_by_model.keys())
    model_names = load_model_names(model_ids)

    out_dir = ensure_output_dir(month)
    print(f"[INFO] 출력 디렉토리: {out_dir}")

    for model_id, tokens in token_by_model.items():
        brand, model_name = model_names.get(model_id, (None, None))

        output_path = build_filename(out_dir, model_id, brand, model_name)
        rel_path = output_path.relative_to(BASE_DIR)

        print(
            f"[INFO] 모델 {model_id} ({brand} {model_name}) "
            f"워드클라우드 생성 → {rel_path}"
        )

        generate_wordcloud_image(
            tokens=tokens,
            output_path=output_path,
            font_path=args.font_path,
            width=args.width,
            height=args.height,
            max_words=args.max_words,
        )

        upsert_blog_wordcloud(
            model_id=model_id,
            month=month,
            image_path=str(rel_path),
        )

    print("[INFO] 워드클라우드 생성/저장/DB upsert 완료")


if __name__ == "__main__":
    main()
