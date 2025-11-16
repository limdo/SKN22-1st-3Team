import streamlit as st
import pandas as pd

def main():
    data = {
        "Model Name": ["Model A", "Model B", "Model C", "Advanced Model D"],
        "Description": [
            "Basic classification model",
            "Text analysis model",
            "Image recognition model",
            "Advanced complex model"
        ],
        "Version": ["1.0", "1.2", "2.0", "3.5"],
        "Status": ["Active", "Active", "Deprecated", "Active"]
    }
    
    df = pd.DataFrame(data)
    
    st.title("Model List")
    
    search_term = st.text_input("Search Models by Name")
    if search_term:
        df = df[df["Model Name"].str.contains(search_term, case=False)]
    
    st.table(df)  # 정적 테이블 출력

if __name__ == "__main__":
    main()