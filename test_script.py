import sys
import psycopg2
import string
import random
import datetime


def generate_insert_row():
    body = '(\'' + ''.join(random.choice(string.ascii_letters) for _ in range(10)) + '\', '
    start = datetime.date(1930, 1, 1)
    end = datetime.date(2021, 1, 1)
    delta = end - start
    arr_date = str(start + datetime.timedelta(days=random.randrange(delta.days))).split('-')
    body += '\'' + arr_date[0] + '/' + arr_date[1] + '/' + arr_date[2] + '\', '
    if random.randrange(2) == 1:
        body += '\'f\')'
    else:
        body += '\'m\')'
    return body


def generate_insert_row_Fm():
    body = '(\'F' + ''.join(random.choice(string.ascii_letters) for _ in range(9)) + '\', '
    start = datetime.date(1930, 1, 1)
    end = datetime.date(2021, 1, 1)
    delta = end - start
    arr_date = str(start + datetime.timedelta(days=random.randrange(delta.days))).split('-')
    body += '\'' + arr_date[0] + '/' + arr_date[1] + '/' + arr_date[2] + '\', '
    body += '\'m\')'
    return body


conn = psycopg2.connect(
    dbname="db1",
    user="uniic",
    password="1234567",
    host="localhost"
)
cursor = conn.cursor()

if str(sys.argv[1]) == '1':
    cursor.execute('''CREATE TABLE Data(
        FIO varchar(80),
        date_birth date,
        sex varchar(3));''')
    conn.commit()
    conn.close()

elif str(sys.argv[1]) == '2':
    cursor.execute('INSERT INTO Data (FIO, date_birth, sex) VALUES (\'' + str(sys.argv[2]) + "\', \'" + str(sys.argv[3]) +
                   "\', \'" + str(sys.argv[4]) + "\');")
    conn.commit()
    conn.close()

elif str(sys.argv[1]) == '3':
    cursor.execute('''SELECT DISTINCT ON (FIO, date_birth) FIO, date_birth, sex, 
    DATE_PART('year',age(date_birth::DATE)) AS age
    FROM Data
    ORDER BY FIO;''')
    res = cursor.fetchall()
    conn.commit()
    conn.close()
    for row in res:
        print(row)

elif str(sys.argv[1]) == '4':
    '''Занесение в базу данных 1000000 случайных данных'''
    for _ in range(200):
        start_req = 'INSERT INTO Data (FIO, date_birth, sex) VALUES '
        body_req = ''
        for _ in range(49999):
            body_req += generate_insert_row() + ', '
        start_req += body_req + generate_insert_row() + ";"
        cursor.execute(start_req)
    conn.commit()
    '''Занесение в базу данных 100 случайных записей, где ФИО начинается с F и пол - муж.'''
    for _ in range(100):
        start_req = 'INSERT INTO Data (FIO, date_birth, sex) VALUES '
        body_req = ''
        for _ in range(99):
            body_req += generate_insert_row_Fm() + ', '
        start_req += body_req + generate_insert_row_Fm() + ";"
        cursor.execute(start_req)
        conn.commit()
    conn.close()

elif str(sys.argv[1]) == '5':
    start_time = datetime.datetime.now()
    request = '''SELECT FIO, date_birth, sex  
    FROM Data
    WHERE FIO ILIKE \'f%\' AND sex = \'m\';'''
    cursor.execute(request)
    end_time = datetime.datetime.now()
    res = cursor.fetchall()
    conn.commit()
    conn.close()
    for row in res:
        print(row)
    time_result = end_time - start_time
    print(str(time_result))

elif str(sys.argv[1]) == '6':
    request_ext = 'CREATE EXTENSION pg_trgm;'
    '''Еслив базе данныхне не установлен pg_trgm, тогда необходимо его установить, иначе будет выведена ошибка. 
    Для этого нужно раскомменитровать две следующие строки'''
    # cursor.execute(request_ext)
    # conn.commit()
    request_index = 'CREATE INDEX FIO_index ON Data USING GIN(FIO gin_trgm_ops);'
    cursor.execute(request_index)
    conn.commit()
    request_index = 'CREATE INDEX sex_index ON Data USING GIN(sex gin_trgm_ops);'
    cursor.execute(request_index)
    conn.commit()
    start_time = datetime.datetime.now()
    request = '''SELECT FIO, date_birth, sex  
        FROM Data
        WHERE FIO ILIKE \'f%\' AND sex ILIKE \'m\';'''
    cursor.execute(request)
    end_time = datetime.datetime.now()
    res = cursor.fetchall()
    conn.commit()
    cursor.execute('DROP INDEX FIO_index;')
    cursor.execute('DROP INDEX sex_index;')
    conn.close()
    for row in res:
        print(row)
    time_result = end_time - start_time
    print(str(time_result))
