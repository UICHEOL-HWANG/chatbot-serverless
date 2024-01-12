import json
import pymysql
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup 
import requests

def lambda_handler(event, context):
    # 사용자 정보 및 검색어 추출
    user_id = event['userRequest']['user']['id']
    search_query = event["userRequest"]["utterance"].split(" ")[0].strip()

    # 사용자의 테이블이 있는지 확인하고 없으면 메시지 반환
    connection = connect_to_rds()
    user_table = f"user_{user_id[-4:]}"

    try:
        with connection.cursor() as cursor:
            if not table_exists(cursor, user_table):
                return {
                    "version": "2.0",
                    "template": {
                        "outputs": [
                            {
                                "simpleText": {
                                    "text": "아직 데이터 세팅이 완료되지 않았습니다. 데이터를 구축해주세요."
                                }
                            }
                        ]
                    }
                }

            # 검색 쿼리를 수행하여 결과 반환
            select_query = f"SELECT * FROM `{user_table}` WHERE `comment` LIKE %s ;"
            cursor.execute(select_query, ('%' + search_query + '%',))
            result = cursor.fetchall()

            if result:
                # 결과가 있으면 ListCard 형태로 반환
                return format_response(result, search_query)
            else:
                # 결과가 없으면 크롤링을 통해 데이터 주입 후 결과 반환
                crawled_data = crawl_news(search_query)
                
                # 트랜잭션을 사용하여 데이터 삽입
                connection.begin()
                try:
                    for data in crawled_data:
                        cursor.execute(
                            f"INSERT INTO `{user_table}` (`rank`, `url`, `image_src`, `title`, `desk`, `comment`, `crawl_datetime`) "
                            f"VALUES (%s, %s, %s, %s, %s, %s, %s)",
                            (data['rank'], data['url'], data['image_src'], data['title'], data['desk'], data['comment'], data['crawl_datetime'])
                        )
                    connection.commit()
                except pymysql.MySQLError as e:
                    print(f"Error during insertion: {e}")
                    connection.rollback()
                finally:
                    connection.close()

                # 다시 검색을 수행하여 결과 반환
                connection = connect_to_rds()
                with connection.cursor() as cursor:
                    cursor.execute(select_query, ('%' + search_query + '%',))
                    result = cursor.fetchall()

                # 결과가 있으면 ListCard 형태로 반환
                return format_response(result, search_query)

    finally:
        connection.close()

# 나머지 함수들은 동일하게 유지됩니다.


def connect_to_rds():
    # RDS 연결 설정
    connection = pymysql.connect(
        host='host',
        user='user',
        password='pw',
        database='db',
        port=3306,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

    return connection

def table_exists(cursor, table_name):
    show_tables_query = f"SHOW TABLES LIKE '{table_name}';"
    cursor.execute(show_tables_query)
    return cursor.rowcount > 0

def crawl_news(search_query):
    # 다음 뉴스 검색 결과 페이지 URL 생성
    url = f"https://search.daum.net/search?nil_suggest=btn&w=news&DA=SBC&cluster=y&q={search_query}"
    r = requests.get(url)
    soup = BeautifulSoup(r.text, "html.parser")

    # 한국 시간대 설정
    korea_timezone = timezone(timedelta(hours=9))
    now_korea = datetime.now(korea_timezone)
    today_datetime = now_korea.strftime("%Y-%m-%d %H:%M:%S")

    # 뉴스 데이터 추출 및 구성
    news_data = []
    for j, i in enumerate(soup.select(".c-list-basic>li")):
        rank = j + 1
        desk = i.select_one(".c-tit-doc>.area_tit>.inner_header>a>.tit_item>span").text if i.select_one(".c-tit-doc>.area_tit>.inner_header>a>.tit_item>span") is not None else ""
        title = i.select_one(".tit-g.clamp-g>a").text if i.select_one(".tit-g.clamp-g>a") is not None else ""
        url = i.select_one(".tit-g.clamp-g>a")["href"] if i.select_one(".tit-g.clamp-g>a") is not None else ""
        comment = i.select_one(".item-contents>p>a").text if i.select_one(".item-contents>p>a") is not None else ""

        news_data.append({
            "rank": rank,
            "url": url,
            "image_src": i.select_one(".wrap_thumb>a>img")["data-original-src"] if i.select_one(".wrap_thumb>a>img") is not None else "",
            "title": title,
            "desk": desk,
            "comment": comment,
            "crawl_datetime": today_datetime
        })

    return news_data

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