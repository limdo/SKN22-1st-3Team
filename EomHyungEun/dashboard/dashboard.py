import streamlit as st
import matplotlib as plt
import pandas as pd
from numpy.random import default_rng as rng

# ----------------------
# PAGE TITLE
# ----------------------
st.set_page_config(layout="wide")

col5, col6, col7 = st.columns([2, 3, 1])

with col6:
    st.title("Car Market Trends Analysis")

st.markdown("---")

# ----------------------
# MAIN DASHBOARD HEADING
# ----------------------
st.header("Main Dashboard Heading")
st.write("")

# ----------------------
# TOP: FILTERS + GRAPH AREA
# ----------------------
col1, col2 = st.columns([1, 3])

with col1:
    st.subheader("Manufacturer")
    option1 = st.selectbox(
        "How would you like to be contacted?",
        ("Email1", "Home phone1", "Mobile phone1"),
    )

    option2 = st.selectbox(
        "How would you like to be contacted?",
        ("Email2", "Home phone2", "Mobile phone2"),
    )

    option3 = st.selectbox(
        "How would you like to be contacted?",
        ("Email3", "Home phone3", "Mobile phone3"),
    )

    option4 = st.selectbox(
        "How would you like to be contacted?",
        ("Email4", "Home phone4", "Mobile phone4"),
    )

with col2:
    st.subheader("GRAPH")

    df = pd.DataFrame(rng(0).standard_normal((20, 3)), columns=["a", "b", "c"])

    st.line_chart(df)


# ----------------------
# WORD CLOUD SECTION
# ----------------------
st.subheader("WORD CLOUD")
st.image("sunrise.jpg", caption="Sunrise by the mountains")

# ----------------------
# BOTTOM: BLOG + SEARCH
# ----------------------
col3, col4 = st.columns(2)

with col3:
    st.subheader("BLOG REVIEWS")
    # 

with col4:
    st.subheader("SEARCH TRENDS")
    st.write("채워야함 ㅠㅠ")
    