import streamlit as st
from pathlib import Path
import base64


def load_css():
    css_path = Path(__file__).parent / "styles" / "main.css"
    if css_path.exists():
        with open(css_path, "r", encoding="utf-8") as f:
            css = f"<style>{f.read()}</style>"
            st.markdown(css, unsafe_allow_html=True)
    else:
        st.error(f"❌ CSS 파일을 찾을 수 없습니다: {css_path}")
