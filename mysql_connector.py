import pandas as pd
import pymysql


def db_connect():
    con = pymysql.connect(host='localhost', user='taeheon',
                          password='rlaxogjs8312', db='movie_db', charset='utf8')
    cur = con.cursor(pymysql.cursors.DictCursor)
    return con, cur


conn, curs = db_connect()

table_queries = [
    """
    DROP TABLE IF EXISTS country;
    """,
    """
    DROP TABLE IF EXISTS movie_director;
    """,
    """
    DROP TABLE IF EXISTS genre;
    """,
    """
    DROP TABLE IF EXISTS director;
    """,
    """
    DROP TABLE IF EXISTS movie;
    """,
    """
    CREATE TABLE movie (
      id INT PRIMARY KEY AUTO_INCREMENT,
      movie_name_ko VARCHAR(255),
      movie_name_en VARCHAR(255),
      release_year VARCHAR(50),
      type VARCHAR(50),
      release_state VARCHAR(50),
      release_company VARCHAR(100)
    )
    """,
    """
    CREATE TABLE director (
      id INT PRIMARY KEY AUTO_INCREMENT,
      name VARCHAR(255)
    )
    """,
    """
    CREATE TABLE genre (
      id INT PRIMARY KEY AUTO_INCREMENT,
      name VARCHAR(255),
      movie_id INT,
      FOREIGN KEY (movie_id) REFERENCES movie(id)
    )
    """,
    """
    CREATE TABLE movie_director (
      movie_id INT,
      director_id INT,
      PRIMARY KEY (movie_id, director_id),
      FOREIGN KEY (movie_id) REFERENCES movie(id),
      FOREIGN KEY (director_id) REFERENCES director(id)
    )
    """,
    """
    CREATE TABLE country(
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100),
    movie_id INT,
    FOREIGN KEY (movie_id) REFERENCES movie(id)
    )
    """
]


# 테이블 생성 함수
def create_tables():
    for query in table_queries:
        curs.execute(query)
    conn.commit()


# create_tables()


def db_insert():
    df = pd.read_excel("movie_data.xls")
    df = df.where(pd.notnull(df), None)
    sql_movie_insert = "insert into movie (id, movie_name_ko, movie_name_en, release_year, type, release_state, release_company) " \
                       "values (%s, %s, %s, %s, %s, %s, %s)"
    sql_director_insert = "insert IGNORE into director (name) values (%s)"
    sql_genre_insert = "insert into genre (movie_id, name) values (%s, %s)"
    sql_movie_director_insert = "insert IGNORE into movie_director (movie_id, director_id) values (%s, %s)"
    sql_country_insert = "insert into country (movie_id, name) values (%s, %s)"
    movie_list = []
    director_list = set()
    genre_list = []
    country_list = []
    movie_id_director_name = []

    for index, row in df.iloc[4:].iterrows():  # 65534개
        movie_list.append(
            (index - 3, row[0], row[1], row[2], row[4], row[6], row[8]))  # row[5] : 장르, row[7]: 감독
        if not pd.isnull(row[5]):
            genre_split = row[5].split(",")
        # 장르를 split해서 넣어주기
        for gs in genre_split:
            genre_list.append((index - 3, gs))
        if not pd.isnull(row[7]):
            director_split = row[7].split(",")
        # 감독을 split해서 넣어주기
        for ds in director_split:
            movie_id_director_name.append((index - 3, ds))
            director_list.add(ds)
        if not pd.isnull(row[3]):
            country_split = row[3].split(",")
        # 나라를 split해서 넣어주기
        for cs in country_split:
            country_list.append((index - 3, cs))
    curs.executemany(sql_movie_insert, movie_list)
    curs.executemany(sql_director_insert, list(director_list))
    curs.executemany(sql_genre_insert, genre_list)
    curs.executemany(sql_country_insert, country_list)

    movie_id_director_id = []
    for a in movie_id_director_name:
        movie_id = a[0]
        curs.execute("SELECT id FROM director WHERE name = %s", a[1])
        director_id = curs.fetchone()['id']
        movie_id_director_id.append((movie_id, director_id))
    curs.executemany(sql_movie_director_insert, movie_id_director_id)
    conn.commit()


# db_insert()

def db_select_first():
    print("1번 문제 : 2020년에 제작된 다큐멘터리 한국 영화의 영화명과 감독을 영화명 순으로 검색하라")
    print("--------------------------------------------------------------")
    sql = """
    SELECT m.movie_name_ko as '영화 이름', d.name as '감독 이름'
    FROM movie m
    JOIN movie_director md ON m.id = md.movie_id
    JOIN director d ON md.director_id = d.id
    JOIN genre g ON g.movie_id = m.id
    JOIN country c ON c.movie_id = m.id
    WHERE m.release_year = '2020' 
      AND g.name = '다큐멘터리'
      AND c.name = '한국'
    ORDER BY m.movie_name_ko ASC, m.movie_name_en ASC;
"""
    curs.execute(sql)
    row = curs.fetchone()
    while row:
        print(f"[영화 이름] : {row['영화 이름']}\n[감독 이름] : {row['감독 이름']}")
        print()
        row = curs.fetchone()
    print()
    print()


def db_select_second():
    print("2번 문제 : 봉준호 감독의 영화를 제작년도 순으로 검색하라")
    print("--------------------------------------------------------------")
    sql = """
    SELECT m.movie_name_ko as '영화 이름', m.release_year as '제작년도'
    FROM movie m
    JOIN movie_director md ON m.id = md.movie_id
    JOIN director d ON md.director_id = d.id
    WHERE d.name = '봉준호'
    ORDER BY m.release_year ASC;
"""
    curs.execute(sql)
    row = curs.fetchone()
    while row:
        print(f"[영화 이름] : {row['영화 이름']} [제작년도] : {row['제작년도']}")
        row = curs.fetchone()
    print()
    print()


def db_select_third():
    print("3번 문제 : 각 년도별 제작된 영화의 편수를 검색하되 년도별로 출력하라")
    sql = """
    SELECT m.release_year as '제작년도', COUNT(*) AS 개수
    FROM movie m
    GROUP BY m.release_year
    ORDER BY m.release_year ASC;
"""
    curs.execute(sql)
    row = curs.fetchone()
    while row:
        if row['제작년도'] is None:
            print(f"[제작년도] : 정보 없음 [개수] : {row['개수']}")
        else:
            print(f"[제작년도] : {row['제작년도']} [개수] : {row['개수']}")
        row = curs.fetchone()
    print()
    print()


def db_select_fourth():
    print("4번 문제 : 각 나라별 가장 많은 제작된 영화 장르를 검색하라")
    sql = """
    SELECT c.name AS '나라이름', g.name AS '장르이름', COUNT(*) AS 개수 
    FROM country c
    JOIN movie m ON c.movie_id = m.id
    JOIN genre g ON m.id = g.movie_id
    GROUP BY c.name, g.name
    HAVING COUNT(*) = (
        SELECT MAX(movie_count)
        FROM (
            SELECT c2.name AS country_name, g2.name AS genre_name, COUNT(*) AS movie_count
            FROM country c2
            JOIN movie m2 ON c2.movie_id = m2.id
            JOIN genre g2 ON m2.id = g2.movie_id
            GROUP BY c2.name, g2.name
        ) AS temp
        WHERE temp.country_name = c.name
    )
"""
    curs.execute(sql)
    row = curs.fetchone()
    while row:
        print(f"[나라 이름] : {row['나라이름']} [장르 이름] : {row['장르이름']} [개수] : {row['개수']}")
        row = curs.fetchone()
    print()
    print()


def db_select_fifth():
    print("5번 문제 : 한국 일본 중국 세 나라에 대하여 각 나라별 영화를 가장 많이 감독한 감독의 이름을 검색하라")
    sql = """
    SELECT c.name AS country_name, d.name AS director_name, COUNT(*) AS movie_count
    FROM country c
    JOIN movie m ON c.movie_id = m.id
    JOIN movie_director md ON m.id = md.movie_id
    JOIN director d ON md.director_id = d.id
    WHERE c.name IN ('한국', '일본', '중국')
    GROUP BY c.name, d.name
    HAVING COUNT(*) = (
        SELECT MAX(cnt)
        FROM (
            SELECT c2.name AS country_name, d2.name AS director_name, COUNT(*) AS cnt
            FROM country c2
            JOIN movie m2 ON c2.movie_id = m2.id
            JOIN movie_director md2 ON m2.id = md2.movie_id
            JOIN director d2 ON md2.director_id = d2.id
            WHERE c2.name IN ('한국', '일본', '중국')
            GROUP BY c2.name, d2.name
        ) AS temp
        WHERE temp.country_name = c.name
    )

"""
    curs.execute(sql)
    row = curs.fetchone()
    while row:
        print(row)
        row = curs.fetchone()
    print()
    print()


# db_select_first()
# db_select_second()
# db_select_third()
# db_select_fourth()
# db_select_fifth()

def search_movies(title='', release_year='', country='', genre='', director=''):
    # 쿼리 생성
    query = "SELECT m.movie_name_ko, m.movie_name_en, m.release_year, m.type, m.release_state, m.release_company, c.name AS country, " \
            "GROUP_CONCAT(DISTINCT g.name) AS genres, GROUP_CONCAT(DISTINCT d.name) AS directors"
    query += " FROM movie m"
    query += " JOIN country c ON m.id = c.movie_id"
    query += " JOIN genre g ON m.id = g.movie_id"
    query += " JOIN movie_director md ON m.id = md.movie_id"
    query += " JOIN director d ON md.director_id = d.id"
    query += " WHERE 1=1"

    if title:
        query += f" AND (m.movie_name_ko LIKE '%{title}%' OR m.movie_name_en LIKE '%{title}%')"
    if release_year:
        query += f" AND m.release_year = '{release_year}'"
    if country:
        query += f" AND c.name = '{country}'"
    if genre:
        query += f" AND g.name = '{genre}'"
    if director:
        query += f" AND d.name = '{director}'"

    query += " GROUP BY m.movie_name_ko, m.movie_name_en, m.release_year, m.type, m.release_state, m.release_company, c.name"

    # 쿼리 실행
    curs.execute(query)
    movies = curs.fetchall()

    if movies:
        print("검색 결과:")
        for movie in movies:
            print("영화 한국어(영어) 제목 :", movie['movie_name_ko'], movie['movie_name_en'])
            print("제작 연도 :", movie['release_year'])
            print("제작 국가 :", movie['country'])
            print("장르 :", movie['genres'])
            print("감독 :", movie['directors'])
            print("영화 유형 :", movie['type'])
            print("개봉 여부 :", movie['release_state'])
            print("제작사 :", movie['release_company'])
            print("--------------------")
    else:
        print("검색 결과가 없습니다.")


# 사용자 입력을 받아 검색 조건 설정
title = input("영화 제목을 입력하세요 (한국어 제목 or 영어 제목): ")
release_year = input("제작 연도를 입력하세요 (없거나 모르겠으면 Enter): ")
country = input("제작 국가를 입력하세요 (없거나 모르겠으면 Enter): ")
genre = input("장르를 입력하세요 (없거나 모르겠으면 Enter): ")
director = input("감독을 입력하세요 (없거나 모르겠으면 Enter): ")

# 영화 검색 함수 호출
search_movies(title, release_year, country, genre, director)

curs.close()
conn.close()
