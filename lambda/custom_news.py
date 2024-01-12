import json
import pymysql
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup 
import requests

def lambda_handler(event, context):
    # 사용자 정보 추출
    user_id = event['userRequest']['user']['id']

    # 사용자의 테이블이 있는지 확인하고 없으면 생성
    connection = connect_to_rds()
    table_name = f"user_{user_id[-4:]}"

    with connection.cursor() as cursor:
        # 테이블이 이미 존재하는지 확인
        if not table_exists(cursor, table_name):
            # 테이블이 없으면 생성
            create_table(cursor, table_name)
            welcome_message = "환영합니다 회원님! 맞춤 뉴스 수집을 위한 데이터 세팅을 완료하였습니다."
        else:
            # 테이블이 이미 존재하면 환영 메시지 출력
            welcome_message = "반갑습니다 회원님! 지금부터 뉴스 수집을 시작하겠습니다."

    return {
        "version": "2.0",
        "template": {
            "outputs": [
                {
                    "simpleText": {
                        "text": welcome_message
                    }
                }
            ]
        }
    }

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

def create_table(cursor, table_name):
    create_table_query = f"CREATE TABLE `{table_name}` (" \
                         f"id INT AUTO_INCREMENT PRIMARY KEY, " \
                         f"`rank` INT, " \
                         f"url TEXT, " \
                         f"image_src TEXT, " \
                         f"title VARCHAR(100), " \
                         f"desk VARCHAR(25), " \
                         f"`comment` VARCHAR(255), " \
                         f"crawl_datetime DATE);"
    cursor.execute(create_table_query)