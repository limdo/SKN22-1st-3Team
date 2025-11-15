# components/kpi.py
import streamlit as st

def kpi_card(label, value, delta=None):
    st.metric(label, value, delta)

def kpi_row(kpis: dict):
    cols = st.columns(len(kpis))
    for (label, value), col in zip(kpis.items(), cols):
        with col:
            if isinstance(value, tuple):
                st.metric(label, value[0], value[1])
            else:
                st.metric(label, value)