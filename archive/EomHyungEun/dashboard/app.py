import streamlit as st
from style_loader import load_css
from streamlit_option_menu import option_menu

import Home
import ModelList
import ModelDetail

st.set_page_config(
    page_title="Dashboard",
    page_icon="📊",
    layout="wide"
)

load_css()
 

# Sidebar
with st.sidebar:
    st.image("https://streamlit.io/images/brand/streamlit-mark-color.png", width=70) # 사이드바 제목 위 아이콘

    st.markdown("<h2 class='sidebar-title'> 사이드 바 </h2>", unsafe_allow_html=True) # 사이드바 제목

    selected = option_menu(
        menu_title=None,
        options=["Home", "ModelList", "ModelDetail"],
        icons=["house", "file-earmark-text", "bar-chart-line"],
        menu_icon="cast",
        default_index=0,
        styles={
            "nav-link": {"font-size": "16px", "padding": "10px"},
            "nav-link-selected": {"background-color": "#1f6feb"},
        }
    )

if selected == "Home":
    Home.main()

elif selected == "ModelList":
    ModelList.main()

elif selected == "ModelDetail":
    ModelDetail.main()
