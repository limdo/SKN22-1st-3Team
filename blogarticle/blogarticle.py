# BLOG_ARTICLE 함수화
# query,model_id 선행 선언 필요
# clident_id, client_secret 선행 선언 필요
# display(블로그 기사 수, int, 1~10) 선언 필요
import requests
from datetime import datetime

def BLOG_ARTICLE(query,model_id,display):

    source = "naver_blog"

    headers = {
        "X-Naver-Client-Id": client_id,
        "X-Naver-Client-Secret": client_secret
    }

    blog_articles = []
    now = datetime.now().isoformat()

    for search_keyword in query:
        params = {
            "query": search_keyword,
            "display": display,
            "sort": "sim"
        }
        url = "https://openapi.naver.com/v1/search/blog.json"
        response = requests.get(url, headers=headers, params=params)
        data = response.json()

        for item in data.get('items', []):
            article = {}
            article['model_id'] = model_id
            article['source'] = source
            article['search_keyword'] = search_keyword
            article['title'] = item.get('title', '')
            article['url'] = item.get('link', '')
            article['description'] = item.get('description', '')
            article['images'] = []  # 대표이미지는 추가 후처리 필요
            article['bloggername'] = item.get('bloggername', '')
            article['bloggerlink'] = item.get('bloggerlink', '')
            pd = item.get('postdate', '')
            if pd and len(pd) == 8:
                article['posted_at'] = f"{pd[:4]}-{pd[4:6]}-{pd[6:]}"
                article['posted_at_raw'] = pd
            else:
                article['posted_at'] = ''
                article['posted_at_raw'] = ''
            article['collected_at'] = now
            blog_articles.append(article)

    # 결과 출력 (필요시 주석 처리하고 return blog_articles로만 사용 가능)
    for i, a in enumerate(blog_articles, 1):
        print(f"==== {i} ====")
        print(f"  search_keyword: {a['search_keyword']}")
        print(f"  model_id: {a['model_id']}")
        print(f"  source:   {a['source']}")
        print(f"  title:    {a['title']}")
        print(f"  url:      {a['url']}")
        print(f"  summary:  {a['description']}")
        print(f"  blogger:  {a['bloggername']} ({a['bloggerlink']})")
        print(f"  posted_at:{a['posted_at']} (raw: {a['posted_at_raw']})")
        print(f"  collected_at: {a['collected_at']}")
        print()

    return blog_articles

# 사용 예시:
# result = BLOG_ARTICLE(["쏘렌토", "싼타페"])
