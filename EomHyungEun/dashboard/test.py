import streamlit as st

col1 = st.columns(1)[0]
with col1:
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

    