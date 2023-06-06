import pandas as pd
import pymysql
def db_connect():
    con = pymysql.connect(host='localhost', user='taeheon',
                          password='rlaxogjs8312', db='movie', charset='utf8')
    cur = con.cursor(pymysql.cursors.DictCursor)
    return con, cur


conn, curs = db_connect()


def db_insert():
    sql_insert = "insert into student (sno, sname, dept) values (%s, %s, %s)"
    # a = (7000, '김선경', '컴퓨터')
    # b = (8000, '황산성', '산업공학')
    # c = (9000, '김호일', '경영학')
    # student_list = [a, b, c]

    # curs.executemany(sql_insert, student_list)
    # conn.commit()


def get_movie_db_excel():
    df = pd.read_excel("movie_data.xls")
    print(df)


get_movie_db_excel()


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
