# components/charts.py
import streamlit as st
import plotly.express as px
import pandas as pd

def line_chart(df, x, y, title=""):
    fig = px.line(df, x=x, y=y, title=title)
    st.plotly_chart(fig, use_container_width=True)

def bar_chart(df, x, y, title=""):
    fig = px.bar(df, x=x, y=y, title=title)
    st.plotly_chart(fig, use_container_width=True)

def pie_chart(df, names, values, title=""):
    fig = px.pie(df, names=names, values=values, title=title)
    st.plotly_chart(fig, use_container_width=True)

def scatter_chart(df, x, y, color=None, size=None, title=""):
    fig = px.scatter(df, x=x, y=y, color=color, size=size, title=title)
    st.plotly_chart(fig, use_container_width=True)

def histogram(df, x, title=""):
    fig = px.histogram(df, x=x, title=title)
    st.plotly_chart(fig, use_container_width=True)
    