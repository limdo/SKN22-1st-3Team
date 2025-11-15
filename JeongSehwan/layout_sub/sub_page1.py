import streamlit as st
from vega_datasets import data


st.title("Model Popularity")

source = data.barley()

st.bar_chart(source, x="year", y="yield", color="site", stack=False)



# streamlit 레이아웃 활용 예시
col1, col2 = st.columns([1, 3])

with col1:
    st.image("https://picsum.photos/200/150")

with col2:
    st.subheader("이 블로그 리뷰는 정말 유용합니다!")
    st.write("블로그 리뷰 보러가기 ⬇️")
    st.link_button("리뷰 링크 열기", "https://example.com")


# HTML/CSS로 카드 스타일링
st.markdown("""
<style>
.review-card {
    border: 1px solid #ddd;
    border-radius: 12px;
    padding: 12px;
    margin-bottom: 20px;
    display: flex;
    gap: 16px;
    text-decoration: none;
    color: inherit;
}
.review-card img {
    width: 120px;
    height: 80px;
    object-fit: cover;
    border-radius: 8px;
}
.review-card-title {
    font-size: 18px;
    font-weight: 700;
}
</style>
""", unsafe_allow_html=True)

link = "https://example.com"
title = "이 블로그 리뷰는 정말 유용합니다!"
img_url = "https://picsum.photos/200/150"

st.markdown(
    f"""
    <a class="review-card" href="{link}" target="_blank">
        <img src="{img_url}" />
        <div>
            <div class="review-card-title">{title}</div>
            <div>블로그 리뷰를 보려면 클릭하세요</div>
        </div>
    </a>
    """,
    unsafe_allow_html=True,
)
