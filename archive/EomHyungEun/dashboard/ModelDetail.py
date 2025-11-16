import streamlit as st
import pandas as pd

def main():
    def model_detail_page():
        # 예시 데이터: 각 모델 기본 정보 + 상세 정보
        models = {
            "Model A": {"Description": "Basic classification model", "Sales": 120, "Status": "Active"},
            "Model B": {"Description": "Text analysis model", "Sales": 340, "Status": "Active"},
            "Model C": {"Description": "Image recognition model", "Sales": 50, "Status": "Deprecated"},
            "Advanced Model D": {"Description": "Advanced complex model", "Sales": 210, "Status": "Active"},
        }

        st.title("Model Detail")

        # 모델 선택 박스
        selected_model = st.selectbox("Select a model to see details", list(models.keys()))

        if selected_model:
            detail = models[selected_model]
            # 상세 정보 출력
            st.subheader(f"Details for {selected_model}")
            st.write(f"**Description:** {detail['Description']}")
            st.write(f"**Sales:** {detail['Sales']}")
            st.write(f"**Status:** {detail['Status']}")

    model_detail_page()  # 함수 호출 추가

if __name__ == "__main__":
    main()