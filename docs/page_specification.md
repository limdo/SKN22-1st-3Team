# Page Specification  
국내 자동차 시장 트렌드 분석 대시보드

> 기준 스키마: `car_model`, `car_model_image`, `model_monthly_interest`, `model_monthly_sales`, `blog_article`, `blog_token_monthly`, `blog_wordcloud`

---

## 1. Home 페이지 – 전체 시장 개요

### 1-1. 목적  
- 최신 월 기준으로 현대/기아 주요 모델의  
  - 판매량 상위 모델  
  - 관심도 상위 모델  
  을 한눈에 비교할 수 있는 개요 화면 제공.

### 1-2. 화면 구성 / 기능

1) 최신 월 선택 영역  
- 기본값: 가장 최근 데이터가 존재하는 월  
- 필요 시 셀렉트박스(연-월)로 변경 가능

2) Top 10 판매 모델 바 차트  
- X축: 모델명 (브랜드 + 모델명)  
- Y축: `sales_units`  
- 정렬: 판매량 내림차순  
- 막대를 클릭하면 해당 모델의 상세 페이지로 이동

3) Top 10 관심도 모델 바 차트  
- X축: 모델명  
- Y축: 네이버 검색 지수 `naver_search_index`  
- 정렬: 관심도 내림차순  
- 동일하게 막대 클릭 시 모델 상세 페이지로 이동

4) 브랜드별 판매량 비교 (현대 vs 기아)  
- Bar 또는 Pie 차트  
- 집계: 최신 월 기준  
  - 현대 총 판매량 = 현대 모델들의 `SUM(sales_units)`  
  - 기아 총 판매량 = 기아 모델들의 `SUM(sales_units)`

### 1-3. 사용 테이블 / 데이터셋
- `model_monthly_sales`  
- `model_monthly_interest`  
- `car_model`

---

## 2. Model List 페이지 – 모델 목록

### 2-1. 목적  
- 현대/기아 모델들을 썸네일 이미지와 함께 나열하여  
  - 사용자에게 전체 모델 스펙트럼을 보여주고  
  - 모델 상세 분석 페이지로 진입할 수 있는 진입점 역할 수행.

### 2-2. 화면 구성 / 기능

1) 상단 컨트롤 영역  
- 정렬 기준 선택  
  - 판매량 기준  
  - 관심도 기준  
- 검색 입력창  
  - 모델명(한글) 검색: 부분 일치

2) 모델 카드 리스트  
- 썸네일 이미지  
- 브랜드명  
- 모델명  
- 최신 월 판매량  
- 최신 월 네이버 검색 지수  

3) 카드 클릭 시  
- 해당 모델의 상세 페이지로 이동

### 2-3. 사용 테이블 / 데이터셋
- `car_model`  
- `car_model_image`  
- `model_monthly_sales`  
- `model_monthly_interest`

---

## 3. Model Detail 페이지 – 모델 상세 분석

### 3-1. 목적  
- 특정 모델에 대해  
  - 관심도 추세  
  - 판매 실적 추세  
  - 블로그 인식(텍스트)  
  를 한 화면에서 종합적으로 확인.

### 3-2. 화면 구성 / 기능

1) 상단 모델 헤더  
- 대표 이미지  
- 브랜드명 + 모델명  
- 다나와 모델 상세 링크

2) 관심도 추세 차트  
- 네이버 검색 지수, 구글 트렌드 지수  
- Line chart

3) 판매량 추세 차트  
- 월 판매량  
- Line chart

4) 블로그 상위 3개 글  
- 제목  
- 요약 또는 본문 일부  
- 링크  

5) 키워드 TOP N  
- Bar chart  
- `blog_token_monthly` 기반

6) 워드 클라우드  
- `blog_wordcloud.image_path` 렌더

### 3-3. 사용 테이블 / 데이터셋
- `car_model`  
- `car_model_image`  
- `model_monthly_interest`  
- `model_monthly_sales`  
- `blog_article`  
- `blog_token_monthly`  
- `blog_wordcloud`

---

## 4. Admin / Data Management 페이지 – 데이터 수집 실행

### 4-1. 목적  
- 대시보드에서 사용하는 테이블을 채우는  
  수집/전처리 스크립트를 수동 실행.

### 4-2. 화면 구성 / 기능
- 수집 월 선택  
- 모델 목록 동기화  
- 관심도 수집  
- 판매실적 수집  
- 블로그 분석 실행  
- 실행 로그 표시

### 4-3. 사용 테이블
- 모든 수집 대상 테이블:  
  `car_model`, `car_model_image`, `model_monthly_interest`,  
  `model_monthly_sales`, `blog_article`,  
  `blog_token_monthly`, `blog_wordcloud`

