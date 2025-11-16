import streamlit as st

def breadcrumb(path_list: list):
    html = ' / '.join([
        f'<a href="#">{p}</a>' if i < len(path_list)-1 else f'<span>{p}</span>'
        for i, p in enumerate(path_list)
    ])
    st.markdown(f'<div class="breadcrumb">{html}</div>', unsafe_allow_html=True)
