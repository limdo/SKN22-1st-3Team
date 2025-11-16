# components/images.py
import streamlit as st

# def image_card(title, image_url, caption=None):
#     st.markdown(f"### {title}")
#     st.image(image_url, width="stretch")
#     if caption:
#         st.caption(caption)
#     st.divider()

def image_card(title, image_url, caption=None):
    st.subheader(f"{title}")
    st.image(image_url, width="stretch")
    if caption:
        st.caption(caption)
    st.divider()

def image_grid(image_urls, columns=3):
    cols = st.columns(columns)
    for i, url in enumerate(image_urls):
        with cols[i % columns]:
            st.image(url, width="stretch")
