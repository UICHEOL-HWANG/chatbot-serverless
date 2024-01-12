import requests
from bs4 import BeautifulSoup
import pymysql
from datetime import datetime, timedelta, timezone
import os
import json

def lambda_handler(event, context):
    
    korea_timezone = timezone(timedelta(hours=9))
    now_korea = datetime.now(korea_timezone)
    today_datetime = now_korea.strftime("%Y-%m-%d")
    
    url_point = [
        "https://news.daum.net/breakingnews/society/affair",
        "https://news.daum.net/breakingnews/politics/assembly",
        "https://news.daum.net/breakingnews/economic/finance",
        "https://news.daum.net/breakingnews/culture/life",
        "https://news.daum.net/breakingnews/entertain/topic",
        "https://news.daum.net/breakingnews/digital/internet"
    ]
    
    category = ["사회,사건사고", "국회,정당", "금융", "생활정보", "연예가화제", "IT"]
    
    # RDS 연결 정보
    RDS_HOST = 'Host'
    RDS_PORT = 3306
    RDS_USER = 'user'
    RDS_PASSWORD = 'pw'
    RDS_DB_NAME = 'db'
    
    for url, cate in zip(url_point, category):
        r = requests.get(url)
        soup = BeautifulSoup(r.text, "html.parser")
    
        # RDS 연결 설정
        db_connection = pymysql.connect(
            host=RDS_HOST,
            user=RDS_USER,
            password=RDS_PASSWORD,
            port=RDS_PORT,
            database=RDS_DB_NAME,
            cursorclass=pymysql.cursors.DictCursor
        )
        cursor = db_connection.cursor()
    
        for index, news_item in enumerate(soup.select(".list_news2.list_allnews>li")):
            rank = index + 1  # rank
            category = cate
            title = news_item.select_one(".tit_thumb>a").text
            url = news_item.select_one(".tit_thumb>a")["href"] if news_item.select_one(".tit_thumb>a") is not None else ""
            image_src = news_item.select_one("a>img")["src"] if  news_item.select_one("a>img") is not None else ""

            desk = news_item.select_one(".info_news").text.split(" · ")[0]
            comment = news_item.select_one(".desc_thumb>.link_txt").text.strip()
            crawl_datetime = today_datetime
    
            # 데이터베이스에 데이터 삽입
            insert_query = """
                INSERT INTO daily_news (`rank`, category, title, url, image_src,`desk`, `comment`, crawl_datetime)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            values = (rank, category, title, url, image_src, desk, comment, crawl_datetime)
    
            cursor.execute(insert_query, values)
            db_connection.commit()
    
        # 연결 닫기
        cursor.close()
        db_connection.close()
    
    # 정상적으로 실행되었음을 클라이언트에게 응답
    response = {
        'statusCode': 200,
        'body': json.dumps('Data successfully inserted into RDS.')
    }

    return response