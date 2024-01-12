import json
import requests
from bs4 import BeautifulSoup
import pymysql
from datetime import timezone, timedelta, datetime

# 수정 사항

def lambda_handler(event, context):
    # 사용자 입력값 받기
    user_utterance = event["userRequest"]["utterance"]
    search_query = user_utterance.split("검색")[0].strip()

    # 데이터베이스에서 이미 있는 내용 확인
    existing_data = retrieve_from_database(search_query)

    if existing_data:
        # 이미 있는 내용이 있다면 해당 내용을 출력
        response = format_response(existing_data, search_query)  # search_query 전달
    else:
        # 데이터베이스에 없는 경우 크롤링하여 데이터베이스에 저장하고 결과 출력
        news_data = data_crawler(search_query)
        save_to_database(news_data)
        response = format_response(news_data, search_query)  # search_query 전달

    return response
    

def retrieve_from_database(data):
    
    # 데이터베이스 연결 설정
    
    RDS_HOST = 'host'
    RDS_PORT = 3306
    RDS_USER = 'user'
    RDS_PASSWORD = 'pw'
    RDS_DB_NAME = 'db'

    db_connection = pymysql.connect(
        host=RDS_HOST,
        user=RDS_USER,
        password=RDS_PASSWORD,
        port=RDS_PORT,
        database=RDS_DB_NAME,
        cursorclass=pymysql.cursors.DictCursor
    )

    try:
        with db_connection.cursor() as cursor:
            # 데이터베이스에서 검색어에 해당하는 내용을 가져옴
            sql = "SELECT * FROM search_news WHERE comment LIKE %s"
            cursor.execute(sql, ('%' + data + '%',))
            result = cursor.fetchall()
    finally:
        db_connection.close()

    return result

def data_crawler(data):
    url = f"https://search.daum.net/search?nil_suggest=btn&w=news&DA=SBC&cluster=y&q={data}"
    r = requests.get(url)
    soup = BeautifulSoup(r.text, "html.parser")

    korea_timezone = timezone(timedelta(hours=9))
    now_korea = datetime.now(korea_timezone)
    today_datetime = now_korea.strftime("%Y-%m-%d %H:%M:%S")

    news_data = []
    for j, i in enumerate(soup.select(".c-list-basic>li")):
        rank = j + 1
        desk = i.select_one(".c-tit-doc>.area_tit>.inner_header>a>.tit_item>span").text if i.select_one(".c-tit-doc>.area_tit>.inner_header>a>.tit_item>span") is not None else ""
        title = i.select_one(".tit-g.clamp-g>a").text if i.select_one(".tit-g.clamp-g>a") is not None else ""
        url = i.select_one(".tit-g.clamp-g>a")["href"] if i.select_one(".tit-g.clamp-g>a") is not None else ""
        comment = i.select_one(".item-contents>p>a").text if i.select_one(".item-contents>p>a") is not None else ""
        image_src = i.select_one(".wrap_thumb>a>img")["data-original-src"] if  i.select_one(".wrap_thumb>a>img") is not None else ""

        news_data.append({
            "rank": rank,
            "url": url,
            "image_src": image_src,
            "title": title,
            "desk": desk,
            "comment": comment,
            "crawl_datetime": today_datetime
        })

    return news_data


def save_to_database(data):
    # RDS 연결 설정
    RDS_HOST = 'host'
    RDS_PORT = 3306
    RDS_USER = 'user'
    RDS_PASSWORD = 'pw'
    RDS_DB_NAME = 'db'

    db_connection = pymysql.connect(
        host=RDS_HOST,
        user=RDS_USER,
        password=RDS_PASSWORD,
        port=RDS_PORT,
        database=RDS_DB_NAME,
        cursorclass=pymysql.cursors.DictCursor
    )

    try:
        with db_connection.cursor() as cursor:
            # 각 뉴스 정보를 데이터베이스에 저장
            for article in data:
                sql = "INSERT INTO search_news (`rank`, url, image_src, title, desk, `comment`, crawl_datetime) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                cursor.execute(sql, (
                    article["rank"], article["url"], article["image_src"], article["title"], article["desk"],
                    article["comment"], article["crawl_datetime"]))
        db_connection.commit()
    finally:
        db_connection.close()

def format_response(data, search_query):
    # 응답 형식을 구성하는 로직 추가
    # data를 listCard 형식으로 변환하여 반환
    list_card = {
        "version": "2.0",
        "template": {
            "outputs": [
                {
                    "listCard": {
                        "header": {
                            "title": f"원하시는 {search_query} 뉴스 검색 결과"
                        },
                        "items": [
                            {
                                "title": article["title"],
                                "description": f"언론사 : {article['desk']}",
                                "imageUrl": article["image_src"],
                                "link": {
                                    "web": article["url"]
                                }
                            } for article in data[:5]
                        ]
                    }   
                },                {
                    "simpleText": {
                        "text": f"원하시는 뉴스인 {search_query} Top 5를 성공적으로 가져왔습니다."
                    }
                }
            ]
        }
    }

    return list_card


