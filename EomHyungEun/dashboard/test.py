import streamlit as st
import pandas as pd
from numpy.random import default_rng

# 🔹 components
from components.inputs import model_selectbox, year_select
from components.images import image_grid, image_card
from components.charts import line_chart
from components.layout import two_columns_ratio
from components.kpi import kpi_row


# ----------------------
# PAGE TITLE
# ----------------------
st.set_page_config(layout="wide")

col5, col6, col7 = st.columns([2, 3, 1])
with col6:
    st.title("🚗 Car Market Trends Analysis")

st.markdown("---")


# ----------------------
# MAIN DASHBOARD HEADING
# ----------------------
st.header("Main Dashboard Overview")
st.write("")


# ----------------------
# TOP AREA: FILTERS + GRAPH
# ----------------------
col1, col2 = two_columns_ratio(left_ratio=1, right_ratio=3)

# ---------------------- FILTERS ----------------------
with col1:
    st.subheader("🔍 Filters")

    manufacturer = model_selectbox("Select Manufacturer", ["현대", "기아", "르노", "쉐보레"])

    model = model_selectbox("Select Model", ["쏘렌토", "카니발", "셀토스", "스포티지"])

    year = year_select("Select Year", 2017, 2025)
    
    chart = model_selectbox("Select Chart", ["line", "bar"])


# ---------------------- GRAPH ----------------------
with col2:
    st.subheader("📈 Monthly Trend Graph")

    rng = default_rng()
    df = pd.DataFrame(
        rng.standard_normal((20, 3)),
        columns=["a", "b", "c"]
    )

    # line_chart(df, x=df.index, y=["a", "b", "c"], title=f"{model} Trend Chart")

    # 🔹 유저가 선택한 차트 타입에 따라 분기
    if chart == "bar":
        # bar 차트의 경우 일반적으로 단일 y값이 자연스러우므로 a 컬럼만 예시로 사용
        from components.charts import bar_chart
        bar_chart(df, x=df.index, y="a", title=f"{model} Bar Chart")
    else:
        line_chart(df, x=df.index, y=["a", "b", "c"], title=f"{model} Line Chart")

# ----------------------
# IMAGE SECTION
# ----------------------
st.subheader("☁ Word Cloud")

image_card(
    title="Word Cloud Example",
    image_url="https://picsum.photos/id/100/300/200",
    caption="(예시) 외부 URL 이미지"
)


# ----------------------
# BOTTOM: BLOG + SEARCH TRENDS
# ----------------------
col3, col4 = two_columns_ratio(1, 1)

# ---------------------- BLOG REVIEWS ----------------------
with col3:
    st.subheader("📝 Blog Reviews")

    sample_images = [
        "https://picsum.photos/id/101/300/200",
        "https://picsum.photos/id/102/300/200",
        "https://picsum.photos/id/104/300/200",
        "https://picsum.photos/id/103/300/200",
    ]

    image_grid(sample_images, columns=2)


# ---------------------- SEARCH TRENDS ----------------------
with col4:
    st.subheader("🔍 Search Trends")

    search_df = pd.DataFrame(
        rng.standard_normal((12, 1)),
        columns=["search_volume"]
    )
    line_chart(search_df, x=search_df.index, y="search_volume", title="Search Keyword Trend")
