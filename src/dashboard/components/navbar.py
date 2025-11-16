import streamlit as st

def navbar(menu_list: dict):
    st.markdown('<div class="navbar">', unsafe_allow_html=True)
    cols = st.columns(len(menu_list))

    for idx, (name, page_key) in enumerate(menu_list.items()):
        with cols[idx]:
            active = "active" if st.session_state.page == page_key else ""
            if st.button(name, key=f"nav_{page_key}"):
                st.session_state.page = page_key

            st.markdown(
                f'<div class="nav-item {active}">{name}</div>',
                unsafe_allow_html=True
            )

    st.markdown('</div>', unsafe_allow_html=True)
