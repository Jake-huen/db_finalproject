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
    DROP TABLE IF EXISTS movie;
    """,
    """
    CREATE TABLE movie (
      id INT PRIMARY KEY,
      movie_name_ko VARCHAR(255),
      movie_name_en VARCHAR(255),
      release_year VARCHAR(50),
      release_country VARCHAR(50),
      type VARCHAR(50),
      release_state VARCHAR(50),
      release_company VARCHAR(50)
    )
    """,
    """
    DROP TABLE IF EXISTS director;
    """,
    """
    CREATE TABLE director (
      id INT PRIMARY KEY,
      name VARCHAR(255)
    )
    """,
    """
    DROP TABLE IF EXISTS genre;
    """,
    """
    CREATE TABLE genre (
      id INT PRIMARY KEY,
      name VARCHAR(255)
    )
    """
    # """
    # DROP TABLE IF EXISTS movie_director;
    # """,
    # """
    # CREATE TABLE movie_director (
    #   movie_id INT,
    #   director_id INT,
    #   PRIMARY KEY (movie_id, director_id),
    #   FOREIGN KEY (movie_id) REFERENCES movie(id),
    #   FOREIGN KEY (director_id) REFERENCES director(id)
    # )
    # """
]


# 테이블 생성 함수
def create_tables():
    for query in table_queries:
        curs.execute(query)
    conn.commit()
    print("테이블이 잘 생성되었습니다.")


create_tables()


def db_insert():
    df = pd.read_excel("movie_data.xls")
    df = df.where(pd.notnull(df), None)
    sql_movie_insert = "insert into movie (id, movie_name_ko, movie_name_en, release_year, release_country, type, release_state, release_company) values (%s, %s, %s, %s, %s, %s, %s, %s)"
    sql_director_insert = "insert into director (id, name) values (%s, %s)"
    sql_genre_insert = "insert into genre (id, name) values (%s, %s)"
    movie_list = []
    director_list = []
    genre_list = []
    for index, row in df.iloc[4:100].iterrows():
        movie_list.append(
            (index - 3, row[0], row[1], row[2], row[3], row[4], row[6], row[8]))  # row[5] : 장르, row[7]: 감독
        genre_list.append((index - 3, row[5]))
        director_list.append((index - 3, row[7]))
    curs.executemany(sql_movie_insert, movie_list)
    curs.executemany(sql_director_insert, director_list)
    curs.executemany(sql_genre_insert, genre_list)
    conn.commit()

db_insert()


def db_delete():
    sno_list = [7000, 8000, 9000]
    for sno in sno_list:
        sql = 'delete from student where sno = %s' % (sno)
        curs.execute(sql)
        conn.commit()


def db_select():
    sql = "select * from student"
    curs.execute(sql)

    row = curs.fetchone()
    while row:
        print(row)
        row = curs.fetchone()


curs.close()
conn.close()
