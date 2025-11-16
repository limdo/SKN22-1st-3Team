# src/etl/sales/danawa_scraper.py

from __future__ import annotations

import os
import time
import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, List, Tuple
from urllib.parse import urljoin, urlparse, parse_qs

from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver

from src.etl.sales.danawa_selenium import get_driver


Brand = Literal["hyundai", "kia"]

# 팀원 코드에서 쓰던 URL 패턴 (Month=YYYY-MM-00)
BASE_MODEL_TAB_URL = "https://auto.danawa.com/auto/?Work=record&Tab=Model&Month={month}"

# 현대/기아 버튼 XPath (sample.ipynb 주석 기반)
BRAND_BUTTON_XPATH = {
    # 실제 XPath는 페이지 구조 확인 후 필요하면 수정
    "hyundai": "/html/body/div/section/div/div/div[2]/div[3]/div[1]/div[1]/ul/li[1]/button",
    "kia": "/html/body/div/section/div/div/div[2]/div[3]/div[1]/div[1]/ul/li[2]/button",
}


@dataclass
class DanawaRow:
    brand: Brand
    month: str  # "2023-01-00" 같은 형태
    rank: str
    model_name: str
    sales: str
    share: str
    mom: str  # 전월대비 (원문 텍스트)
    yoy: str  # 전년대비 (원문 텍스트)
    detail_url: str | None  # 모델 상세 페이지 URL
    image_url: str | None  # 썸네일 이미지 URL


def extract_model_id_from_url(url: str) -> int | None:
    """
    예: /auto/?Work=model&Model=33191 → 33191
    """
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    vals = qs.get("Model") or qs.get("model")
    if not vals:
        return None
    try:
        return int(vals[0])
    except ValueError:
        return None


def click_brand_tab(driver: WebDriver, brand: Brand) -> None:
    """
    페이지 상단의 '브랜드별 보기'에서 현대/기아 탭 버튼 클릭.
    sample.ipynb의 XPath를 그대로 사용하되, 브랜드별로 분리.
    """
    xpath = BRAND_BUTTON_XPATH.get(brand)
    if not xpath:
        raise ValueError(f"지원하지 않는 브랜드: {brand}")

    # 브랜드별 보기 버튼 클릭
    try:
        brand_btn = driver.find_element(By.XPATH, xpath)
        brand_btn.click()
        print(f"[INFO] 브랜드 탭 클릭 완료: {brand}")
    except Exception as e:
        print(f"[WARN] 브랜드 탭 클릭 실패: {brand}, error={e}")


def scrape_month_for_brand(
    driver: WebDriver,
    brand: Brand,
    month: str,
    scroll_wait: float = 1.0,
    table_timeout: int = 5,
) -> List[DanawaRow]:
    """
    팀원의 sample.ipynb 로직을 함수화:
    - 특정 month, 특정 brand에 대해
      1) URL 접속
      2) 브랜드 탭 클릭
      3) 테이블 로딩 대기
      4) 각 행에서 텍스트 추출
      5) 모델 상세 URL / 이미지 URL도 함께 추출
    """
    print("\n" + "=" * 30)
    print(f"[INFO] {month} / {brand} 데이터 수집 시작")
    print("=" * 30)

    url = BASE_MODEL_TAB_URL.format(month=month)
    driver.get(url)

    # 페이지 기본 로딩 대기
    time.sleep(5)

    # 브랜드 탭 클릭
    click_brand_tab(driver, brand=brand)

    # 스크롤 조금 내려서 렌더링 유도
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
    time.sleep(scroll_wait)

    # 테이블 로딩 대기
    rows_elements = []
    for i in range(table_timeout):
        rows_elements = driver.find_elements(
            By.CSS_SELECTOR, "table.recordTable.model tbody tr"
        )
        if rows_elements:
            print(f"[INFO] {i + 1}초 후 데이터 로드 완료 ({len(rows_elements)}개 행)")
            break
        time.sleep(1)
    else:
        print("[ERROR] Timeout: 테이블 로드 실패")
        return []

    results: List[DanawaRow] = []

    for row in rows_elements:
        tds = row.find_elements(By.CSS_SELECTOR, "td")
        cols = [td.text.strip() for td in tds]

        # 팀원 코드 기준: 항상 8개 column
        if len(cols) != 8:
            continue

        rank = cols[1].strip()
        model_name = cols[3].strip()
        sales = cols[4].strip()
        share = cols[5].strip()

        # 전월대비 텍스트 정리
        mom_raw = cols[6].split("\n")
        mom = " ".join(x.strip() for x in mom_raw if x.strip())

        # 전년대비 텍스트 정리
        yoy_raw = cols[7].split("\n")
        yoy = " ".join(x.strip() for x in yoy_raw if x.strip())

        # 모델명 셀에서 a/img를 다시 가져와 URL 정보 추출
        detail_url = None
        image_url = None
        try:
            # 모델명이 들어 있는 td는 인덱스 3 (0 기반) 이라고 가정
            model_td = tds[3]
            a_el = model_td.find_element(By.CSS_SELECTOR, "a")
            href = a_el.get_attribute("href")
            if href:
                # 절대 URL 보장
                detail_url = href

            try:
                img_el = model_td.find_element(By.CSS_SELECTOR, "img")
                src = img_el.get_attribute("src")
                if src:
                    image_url = src
            except Exception:
                # 이미지 없는 행도 있을 수 있음
                pass
        except Exception:
            # 구조가 조금 달라도 크롤링은 계속 되도록
            pass

        results.append(
            DanawaRow(
                brand=brand,
                month=month,
                rank=rank,
                model_name=model_name,
                sales=sales,
                share=share,
                mom=mom,
                yoy=yoy,
                detail_url=detail_url,
                image_url=image_url,
            )
        )

    print(f"[INFO] {month} / {brand} 행 개수: {len(results)}")
    return results


def save_sales_csv(rows: List[DanawaRow], out_path: Path) -> None:
    """
    팀원 sample.ipynb에서 생성하던 raw CSV 형식 그대로 저장.
    컬럼: 순위, "", 모델명, 판매량, 점유율, 전월대비, 전년대비
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with out_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["순위", "", "모델명", "판매량", "점유율", "전월대비", "전년대비"]
        )
        for r in rows:
            writer.writerow(
                [
                    r.rank,
                    "",
                    r.model_name,
                    r.sales,
                    r.share,
                    r.mom,
                    r.yoy,
                ]
            )
    print(f"[INFO] 판매량 CSV 저장: {out_path}")


def save_meta_csv(rows: List[DanawaRow], out_path: Path) -> None:
    """
    모델 상세 URL / 이미지 URL 메타를 별도 CSV로 저장.
    컬럼: brand, month, rank, model_name, detail_url, image_url
    나중에 car_model / car_model_image 적재할 때 이 파일 쓰면 된다.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with out_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "brand",
                "month",
                "rank",
                "model_name",
                "detail_url",
                "image_url",
            ]
        )
        for r in rows:
            writer.writerow(
                [
                    r.brand,
                    r.month,
                    r.rank,
                    r.model_name,
                    r.detail_url or "",
                    r.image_url or "",
                ]
            )
    print(f"[INFO] 메타 CSV 저장: {out_path}")
