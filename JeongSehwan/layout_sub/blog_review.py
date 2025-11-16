import streamlit as st

# streamlit 레이아웃 활용 예시
st.title("streamlit 레이아웃 활용 예시")
col1, col2 = st.columns([1, 3])

with col1:
    st.image("https://picsum.photos/200/150")

with col2:
    st.subheader("이 블로그 리뷰는 정말 유용합니다!")
    st.write("블로그 리뷰 보러가기 ⬇️")
    st.link_button("리뷰 링크 열기", "https://example.com")


# HTML/CSS로 카드 스타일링
st.title("HTML/CSS로 카드 스타일링")
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




# ------- 카드 스타일 정의 -------
st.title("다수의 카드 스타일링 예시")
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
    transition: 0.2s;
}
.review-card:hover {
    background: #f7f7f7;
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
    margin-bottom: 6px;
}
</style>
""", unsafe_allow_html=True)

# ------- 리뷰 데이터 예시 -------
reviews = [
    {
        "title": "맛집 리뷰 – 신촌 파스타집",
        "link": "https://example.com/1",
        "img": "https://picsum.photos/200/150?random=1"
    },
    {
        "title": "카페 리뷰 – 연남동 감성 카페",
        "link": "https://example.com/2",
        "img": "https://picsum.photos/200/150?random=2"
    },
    {
        "title": "여행 리뷰 – 강릉 1박 2일 코스",
        "link": "https://example.com/3",
        "img": "https://picsum.photos/200/150?random=3"
    }
]

# ------- 반복 출력 -------
for r in reviews:
    st.markdown(
        f"""
        <a class="review-card" href="{r['link']}" target="_blank">
            <img src="{r['img']}" />
            <div>
                <div class="review-card-title">{r['title']}</div>
                <div>리뷰 링크를 보려면 클릭하세요</div>
            </div>
        </a>
        """,
        unsafe_allow_html=True,
    )


# 다른 형태의 카드 스타일링 예시
st.title("오버레이 카드 스타일링 예시")

html = """
<style>
.overlay-grid {
    display: grid;
    grid-template-columns: repeat(2, 1fr);
    gap: 16px;
}
.overlay-card {
    position: relative;
    width: 100%;
    height: 200px;
    border-radius: 14px;
    overflow: hidden;
    cursor: pointer;
    transition: 0.3s;
    display: block;
}
.overlay-card:hover {
    transform: scale(1.03);
}
.overlay-card img {
    width: 100%;
    height: 100%;
    object-fit: cover;
}
.overlay-title-box {
    position: absolute;
    bottom: 0;
    width: 100%;
    padding: 12px;
    background: linear-gradient(to top, rgba(0,0,0,0.7), rgba(0,0,0,0));
}
.overlay-title {
    color: white;
    font-size: 17px;
    font-weight: 700;
}
</style>

<div class="overlay-grid">

<a href="https://example.com/1" target="_blank" class="overlay-card">
    <img src="https://picsum.photos/400/300?random=1">
    <div class="overlay-title-box">
        <div class="overlay-title">신촌 파스타 맛집 방문기🍝</div>
    </div>
</a>

<a href="https://example.com/2" target="_blank" class="overlay-card">
    <img src="https://picsum.photos/400/300?random=2">
    <div class="overlay-title-box">
        <div class="overlay-title">연남동 감성 카페☕</div>
    </div>
</a>

<a href="https://example.com/3" target="_blank" class="overlay-card">
    <img src="https://picsum.photos/400/300?random=3">
    <div class="overlay-title-box">
        <div class="overlay-title">주말 강릉 여행🌊</div>
    </div>
</a>

<a href="https://example.com/4" target="_blank" class="overlay-card">
    <img src="https://picsum.photos/400/300?random=4">
    <div class="overlay-title-box">
        <div class="overlay-title">공원 피크닉 후기🌳</div>
    </div>
</a>

</div>
"""

st.markdown(html, unsafe_allow_html=True)
