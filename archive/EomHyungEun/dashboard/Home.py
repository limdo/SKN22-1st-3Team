import streamlit as st
import pandas as pd
from numpy.random import default_rng

# components
from components.inputs import model_selectbox, year_select
from components.images import image_grid, image_card
from components.charts import line_chart, bar_chart
from components.layout import two_columns_ratio
from style_loader import load_css


# ------------------------------------------------------
# PAGE SETTINGS
# ------------------------------------------------------
st.set_page_config(layout="wide")
load_css()   # CSS 적용

def main():

    st.markdown('<div class="page-wrapper">', unsafe_allow_html=True)


    # ------------------------------------------------------
    # PAGE TITLE
    # ------------------------------------------------------
    st.markdown('<div class="page-title">🚗 Car Market Trends Analysis</div>', unsafe_allow_html=True)
    st.markdown("---")



    # ------------------------------------------------------
    # FILTERS + CHART SECTION
    # ------------------------------------------------------
    col1, col2 = two_columns_ratio(1, 3)

    with col1:
        st.markdown('<div class="filter-title">Filters</div>', unsafe_allow_html=True)

        year = year_select("Select Year", 2017, 2025)
        manufacturer = model_selectbox("Select Manufacturer", ["현대", "기아", "르노", "쉐보레"])
        model = model_selectbox("Select Model", ["쏘렌토", "카니발", "셀토스", "스포티지"])
        chart_type = model_selectbox("Select Chart", ["line", "bar"])

        st.markdown('</div>', unsafe_allow_html=True)


    with col2:
        st.markdown('<div class="section-title">📈 Monthly Trend Graph</div>', unsafe_allow_html=True)

        rng = default_rng()
        df = pd.DataFrame(rng.standard_normal((20, 3)), columns=["a", "b", "c"])

        if chart_type == "bar":
            bar_chart(df, x=df.index, y="a", title=f"{year}년 {manufacturer} {model} 통계")
        else:
            line_chart(df, x=df.index, y=["a", "b", "c"], title=f"{year}년 {manufacturer} {model} 통계")

        st.markdown('</div>', unsafe_allow_html=True)



    # ------------------------------------------------------
    # WORD CLOUD SECTION
    # ------------------------------------------------------
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">☁ Word Cloud</div>', unsafe_allow_html=True)

    image_card(
        title="Word Cloud Example",
        image_url="https://picsum.photos/id/100/300/200",
        caption="자동차 모델 키워드 기반 워드클라우드"
    )

    st.markdown('</div>', unsafe_allow_html=True)



    # ------------------------------------------------------
    # BOTTOM: BLOG & SEARCH
    # ------------------------------------------------------
    col3, col4 = two_columns_ratio(1, 1)


    # BLOG REVIEW
    with col3:
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">📝 Blog Reviews</div>', unsafe_allow_html=True)

        sample_images = [
            "https://picsum.photos/id/101/300/200",
            "https://picsum.photos/id/102/300/200",
            "https://picsum.photos/id/104/300/200",
            "https://picsum.photos/id/103/300/200",
        ]
        image_grid(sample_images, columns=2)

        st.markdown('</div>', unsafe_allow_html=True)



    # SEARCH TRENDS
    with col4:
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">🔍 Search Trends</div>', unsafe_allow_html=True)

        search_df = pd.DataFrame(default_rng().standard_normal((12, 1)), columns=["search_volume"])
        line_chart(search_df, x=search_df.index, y="search_volume", title="Search Keyword Trend")

        st.markdown('</div>', unsafe_allow_html=True)



    st.markdown('<div class="footer-space"></div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)
