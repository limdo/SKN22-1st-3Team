# components/layout.py
import streamlit as st

def two_columns_ratio(left_ratio=1, right_ratio=1):
    return st.columns([left_ratio, right_ratio])

def three_columns():
    return st.columns(3)

