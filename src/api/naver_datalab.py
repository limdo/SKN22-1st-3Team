# src/api/naver_datalab.py

from __future__ import annotations

from typing import List, Dict, Any, Optional
import os
import requests


class NaverDatalabClient:
    BASE_URL = "https://openapi.naver.com/v1/datalab/search"

    def __init__(
        self, client_id: Optional[str] = None, client_secret: Optional[str] = None
    ):
        self.client_id = client_id or os.getenv("NAVER_CLIENT_ID")
        self.client_secret = client_secret or os.getenv("NAVER_CLIENT_SECRET")

        if not self.client_id or not self.client_secret:
            raise RuntimeError(
                "NAVER_DATALAB_CLIENT_ID / NAVER_DATALAB_CLIENT_SECRET 환경변수가 필요합니다."
            )

    def fetch_trend(
        self,
        keyword: str,
        start_date: str,
        end_date: str,
        time_unit: str = "month",
        ages: Optional[List[str]] = None,
        device: Optional[str] = None,
        gender: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        단일 키워드에 대해 네이버 데이터랩 검색 트렌드를 가져온다.
        반환값은 [{"period": "YYYY-MM-DD", "ratio": float}, ...] 형태의 리스트.
        """
        headers = {
            "X-Naver-Client-Id": self.client_id,
            "X-Naver-Client-Secret": self.client_secret,
        }

        body: Dict[str, Any] = {
            "startDate": start_date,
            "endDate": end_date,
            "timeUnit": time_unit,  # "date", "week", "month"
            "keywordGroups": [{"groupName": keyword, "keywords": [keyword]}],
        }

        if ages:
            body["ages"] = ages
        if device:
            body["device"] = device
        if gender:
            body["gender"] = gender

        resp = requests.post(self.BASE_URL, headers=headers, json=body, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        results = data.get("results") or []
        if not results:
            return []

        # 기본적으로 첫 번째 그룹만 사용
        return results[0].get("data", [])
