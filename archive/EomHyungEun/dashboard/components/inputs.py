# components/inputs.py
import streamlit as st

def model_selectbox(label, model_list):
    return st.selectbox(label, model_list)

def year_select(label="연도 선택", start=2015, end=2025):
    years = list(range(start, end + 1))
    return st.selectbox(label, years)

def multi_model_select(label, model_list):
    return st.multiselect(label, model_list)

def date_range_picker(label="기간 선택"):
    return st.date_input(label, [])