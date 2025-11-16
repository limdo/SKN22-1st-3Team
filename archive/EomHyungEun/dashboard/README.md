실행문:
    streamlit run app.py

dashboard/
│   │   ├── app.py                # 실행 앱 
│   │   ├── Home.py               # 메인 페이지
│   │   ├── ModelList.py          # 모델 목록 페이지
│   │   ├── ModelDetail.py        # 상세 분석 페이지
│   │   ├── style_loader.py       # css 불러오는 기능
│   │   ├── components/           # UI 컴포넌트 모음
│   │   │   ├── charts.py
│   │   │   ├── images.py
│   │   │   ├── input.py
│   │   │   ├── layout.py
│   │   │   └── visualize.py
│   │   ├── .streamlit/           
│   │   │   └── config.toml      # streamlit theme 설정
│   │   ├── styles/ 
│   │   │   └── main.css

필요:
    streamlit
    matplotlib
    pandas
    numpy
    plotly.express
    streamlit-option-menu

