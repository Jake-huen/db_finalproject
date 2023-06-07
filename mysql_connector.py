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
    DROP TABLE IF EXISTS movie_director;
    """,
    """
    DROP TABLE IF EXISTS movie_genre;
    """,
    """
    DROP TABLE IF EXISTS director;
    """,
    """
    DROP TABLE IF EXISTS movie;
    """,
    """
    DROP TABLE IF EXISTS genre;
    """,
    """
    CREATE TABLE movie (
      id INT PRIMARY KEY AUTO_INCREMENT,
      movie_name_ko VARCHAR(255),
      movie_name_en VARCHAR(255),
      release_year VARCHAR(50),
      release_country VARCHAR(50),
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
    DROP TABLE IF EXISTS genre;
    """,
    """
    CREATE TABLE genre (
      id INT PRIMARY KEY AUTO_INCREMENT,
      name VARCHAR(255)
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
    CREATE TABLE movie_genre (
      movie_id INTEGER,
      genre_id INTEGER,
      FOREIGN KEY (movie_id) REFERENCES movie(id),
      FOREIGN KEY (genre_id) REFERENCES genre(id)
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
    sql_movie_insert = "insert into movie (movie_name_ko, movie_name_en, release_year, release_country, type, release_state, release_company) " \
                       "values (%s, %s, %s, %s, %s, %s, %s)"
    sql_director_insert = "insert IGNORE into director (name) values (%s)"
    sql_genre_insert = "insert IGNORE into genre (name) values (%s)"
    sql_movie_director_insert = "insert IGNORE into movie_director (movie_id, director_id) values (%s, %s)"
    sql_movie_genre_insert = "insert IGNORE into movie_genre (movie_id, genre_id) values (%s, %s)"
    movie_list = []
    director_list = set()
    genre_list = set()
    movie_name_director_name = []
    movie_name_genre_name = []

    for index, row in df.iloc[4:10000].iterrows():  # 65534개
        print(index)
        movie_list.append(
            (row[0], row[1], row[2], row[3], row[4], row[6], row[8]))  # row[5] : 장르, row[7]: 감독
        if not pd.isnull(row[5]):
            genre_split = row[5].split(",")
        # 장르를 split해서 넣어주기
        for gs in genre_split:
            genre_list.add(gs)
            movie_name_genre_name.append((row[0], gs))
        if not pd.isnull(row[7]):
            director_split = row[7].split(",")
        # 감독을 split해서 넣어주기
        for ds in director_split:
            movie_name_director_name.append((row[0], ds))
            director_list.add(ds)
    # print(movie_list)
    # print(list(director_list))
    # print(list(genre_list))
    curs.executemany(sql_movie_insert, movie_list)
    curs.executemany(sql_director_insert, list(director_list))
    curs.executemany(sql_genre_insert, list(genre_list))

    movie_id_director_id = []
    movie_id_genre_id = []
    for a in movie_name_director_name:
        curs.execute("SELECT id FROM movie WHERE movie_name_ko = %s", a[0])
        movie_id = curs.fetchone()['id']
        curs.execute("SELECT id FROM director WHERE name = %s", a[1])
        director_id = curs.fetchone()['id']
        movie_id_director_id.append((movie_id, director_id))
    # print(movie_id_director_id)
    curs.executemany(sql_movie_director_insert, movie_id_director_id)

    for a in movie_name_genre_name:
        curs.execute("SELECT id FROM movie WHERE movie_name_ko = %s", a[0])
        movie_id = curs.fetchone()['id']
        curs.execute("SELECT id FROM genre WHERE name = %s", a[1])
        genre_id = curs.fetchone()['id']
        movie_id_genre_id.append((movie_id, genre_id))
    # print(movie_id_genre_id)
    curs.executemany(sql_movie_genre_insert, movie_id_genre_id)
    conn.commit()


# db_insert()

def db_select_first():
    print("1번 문제 : 2020년에 제작된 다큐멘터리 한국 영화의 영화명과 감독을 영화명 순으로 검색하라")
    sql = """
    SELECT movie.movie_name_ko as '영화 이름', director.name as '감독 이름'
    FROM movie
    JOIN movie_director ON movie.id = movie_director.movie_id
    JOIN director ON movie_director.director_id = director.id
    JOIN movie_genre ON movie.id = movie_genre.movie_id
    JOIN genre ON movie_genre.genre_id = genre.id
    WHERE movie.release_year = '2020' 
      AND genre.name = '다큐멘터리'
      AND movie.release_country = '한국'
    ORDER BY movie.movie_name_ko ASC;
"""
    curs.execute(sql)
    row = curs.fetchone()
    while row:
        print(row)
        row = curs.fetchone()
    print()
    print()

def db_select_second():
    print("2번 문제 : 봉준호 감독의 영화를 제작년도 순으로 검색하라")
    sql = """
    SELECT movie.movie_name_ko as '영화 이름', movie.release_year as '제작년도'
    FROM movie
    JOIN movie_director ON movie.id = movie_director.movie_id
    JOIN director ON movie_director.director_id = director.id
    WHERE director.name = '봉준호'
    ORDER BY movie.release_year ASC;
"""
    curs.execute(sql)
    row = curs.fetchone()
    while row:
        print(row)
        row = curs.fetchone()
    print()
    print()

def db_select_third():
    print("3번 문제 : 각 년도별 제작된 영화의 편수를 검색하되 년도별로 출력하라")
    sql = """
    SELECT movie.release_year as '제작년도', COUNT(*) AS movie_count
    FROM movie
    GROUP BY movie.release_year
    ORDER BY movie.release_year ASC;
"""
    curs.execute(sql)
    row = curs.fetchone()
    while row:
        print(row)
        row = curs.fetchone()
    print()
    print()

def db_select_fourth():
    print("4번 문제 : 각 나라별 가장 많은 제작된 영화 장르를 검색하라")
    sql = """
    SELECT m.release_country, g.name AS genre, COUNT(*) AS movie_count
    FROM movie m
    JOIN movie_genre mg ON m.id = mg.movie_id
    JOIN genre g ON mg.genre_id = g.id
    GROUP BY m.release_country, g.name
    HAVING COUNT(*) = (
        SELECT MAX(movie_count)
        FROM (
            SELECT release_country, genre_id, COUNT(*) AS movie_count
            FROM movie
            JOIN movie_genre ON movie.id = movie_genre.movie_id
            GROUP BY release_country, genre_id
        ) AS counts
        WHERE counts.release_country = m.release_country
    )
    ORDER BY m.release_country ASC;
"""
    curs.execute(sql)
    row = curs.fetchone()
    while row:
        print(row)
        row = curs.fetchone()
    print()
    print()

def db_select_fifth():
    print("5번 문제 : 한국 일본 중국 세나라에 대하여 각 나라별 영화를 가장 많이 감독한 감독의 이름을 검색하라")
    sql = """
    SELECT m.release_country, g.name AS genre, COUNT(*) AS movie_count
    FROM movie m
    JOIN movie_genre mg ON m.id = mg.movie_id
    JOIN genre g ON mg.genre_id = g.id
    GROUP BY m.release_country, g.name
    HAVING COUNT(*) = (
        SELECT MAX(movie_count)
        FROM (
            SELECT release_country, COUNT(*) AS movie_count
            FROM movie
            GROUP BY release_country
        ) AS counts
        WHERE counts.release_country = m.release_country
    )
    ORDER BY m.release_country ASC;
"""
    curs.execute(sql)
    row = curs.fetchone()
    while row:
        print(row)
        row = curs.fetchone()
    print()
    print()

db_select_first()
db_select_second()
db_select_third()
db_select_fourth()
db_select_fifth()

curs.close()
conn.close()
