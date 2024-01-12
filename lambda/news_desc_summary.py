import json
import pymysql
from datetime import datetime, timedelta, timezone

def lambda_handler(event, context):

    params = event['action']['params']
    category = params['category']


    # 현재 UTC 시간 구하기
    utc_now = datetime.utcnow()

    # UTC를 한국 시간으로 변환
    korea_timezone = timezone(timedelta(hours=9))
    korea_now = utc_now.replace(tzinfo=timezone.utc).astimezone(korea_timezone)

    # korea_now를 문자열로 변환 (날짜까지만)
    formatted_korea_now = korea_now.strftime('%Y-%m-%d')

    # RDS 연결 정보 설정 (자신의 RDS 정보로 변경)
    rds_host = "host"
    name = "user"
    password = "pwd"
    db_name = "db"
    try:
        # RDS 연결
        conn = pymysql.connect(host=rds_host, user=name, password=password, database=db_name, port=3306, connect_timeout=5)

        with conn.cursor() as cursor:
            
            # 입력된 카테고리에 따라 해당 카테고리의 기사 가져오기
            sql = """
                SELECT url, image_src, title, desk
                FROM daily_news
                WHERE DATE(crawl_datetime) = '{}' AND category = '{}'
                ORDER BY `rank` asc 
            """.format(formatted_korea_now, category)
            
            cursor.execute(sql)
            print(sql)
            result = cursor.fetchall()
            result_top5 = result[:5]

            # 결과를 listCard 형식으로 가공
            list_card = {
                "version": "2.0",
                "template": {
                    "outputs": [
                        {
                            "listCard": {
                                "header": {
                                    "title": f"{category} 뉴스 Top 5"
                                },
                                "items": []
                            }
                        }
                    ]
                }
            }

            for row in result_top5:
                item = {
                    "title": row[2],
                    "description": f"언론사 : {row[3]}",
                    "imageUrl": row[1],
                    "link": {
                        "web": row[0]
                    }
                }
                list_card["template"]["outputs"][0]["listCard"]["items"].append(item)

    finally:
        conn.close()
        
    return list_card


    