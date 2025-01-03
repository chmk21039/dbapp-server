from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg
import json
import re
import datetime

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False
cors = CORS(app)

@app.route('/publishers', methods=['GET'])
def get_publishers():
    # データベースに接続
    connection = psycopg.connect(
        host='localhost',
        dbname='book',
        user='postgres',
        password='password',
        port='5433'
        #client_encoding='utf-8'
    )
    cursor = connection.cursor()
    
    # SQLを実行
    sql = '''
    SELECT 出版社番号, 出版社名 FROM 出版社;
    '''
    result = connection.execute(sql)
    publishers = []
    #(4, '筑摩書房')
    for row in result:
        dic = {}
        r = str(row).replace('(', '').replace(')', '').replace('\'', '').replace(' ', '').split(',')
        print(r)
        dic["出版社番号"] = r[0]
        dic["出版社名"] = r[1]
        publishers.append(dic)

    print(publishers)
    
    return jsonify(publishers)


@app.route('/publishers', methods=['POST'])
def post_publisher():
    content = request.get_json()
    connection = psycopg.connect(
    host='localhost',
    dbname='students',
    user='postgres',
    password='password',
    port='5433'
)
    print(content)
    sql = '''
    SELECT MAX(出版社番号) FROM 出版社;
    '''
    result = connection.execute(sql)
    for row in result:
        print(row)
        r = str(row).replace('(', '').replace(')', '').replace('\'', '').replace(' ', '').split(',')
        num = int(r[0])+1
    
    target = content['出版社名']
    print(num, target)
    try:
        sql = '''
        INSERT INTO 出版社 (出版社番号, 出版社名)
        VALUES (%(num)s, %(target)s);
        '''
        print(sql)
        connection.execute(sql, {'num': num, 'target': target})
    except Exception:
        connection.rollback()
    else:
        connection.commit()
    print(content)
    return jsonify({'message': 'created'})


@app.route('/authors', methods=['GET'])
def get_authors():
    connection = psycopg.connect(
        host='localhost',
        dbname='book',
        user='postgres',
        password='password',
        client_encoding='utf-8',
        port='5433'
    )

    sql = '''
    SELECT * FROM 著者;
    '''
    result = connection.execute(sql)
    authors = []
    for row in result:
        dic = {}
        r = str(row).replace('(', '').replace(')', '').replace('\'', '').replace(' ', '').split(',')
        dic["著者番号"] = r[0]
        dic["著者名"] = r[1]
        authors.append(dic)
    return jsonify(authors)


@app.route('/authors', methods=['POST'])
def post_author():
    content = request.get_json()
    print(content)
    connection = psycopg.connect(
    host='localhost',
    dbname='students',
    user='postgres',
    password='password',
    port='5433'
    )
    sql = '''
    SELECT MAX(著者番号) FROM 著者;
    '''
    result = connection.execute(sql)
    for row in result:
        r = str(row).replace('(', '').replace(')', '').replace('\'', '').replace(' ', '').split(',')
        num = int(r[0])+1
    target = content['著者名']
    print(num, target)
    try:
        sql = '''
        INSERT INTO 著者 (著者番号, 著者名)
        VALUES (%(num)s, %(target)s);
        '''
        connection.execute(sql, {'num': num, 'target': target})
    except Exception:
        connection.rollback()
    else:
        connection.commit()
    return jsonify({'message': 'created'})


@app.route('/books', methods=['GET'])
def get_books():
    connection = psycopg.connect(
        host='localhost',
        dbname='book',
        user='postgres',
        password='password',
        port='5433'
    )
    sql = '''
    SELECT * FROM 書籍 JOIN 書籍著者 ON 書籍."ISBNコード" = 書籍著者."ISBNコード"
    JOIN 著者 ON 書籍著者.著者番号 = 著者.著者番号
    JOIN 出版社 ON 書籍.出版社番号 = 出版社.出版社番号;
    '''
    result = connection.execute(sql)
    books = {}
    for entry in result:
        fields = str(entry).replace('(', '').replace(')', '').replace(' ', '').replace('\\u3000', '　').split(',')
        isbn = fields[0]
        book = books.setdefault(isbn, {
            'ISBNコード': isbn,
            '書籍名': fields[1],
            '著者': [],
            '出版社': {
                '出版社番号': fields[9],
                '出版社名': fields[10]
            },
            '出版年': fields[3]
        })
        book['著者'].append({
            '著者番号': fields[7],
            '著者名': fields[8],
            '役割': fields[6]
        })

    books_j = list(books.values())
    return jsonify(books_j)


@app.route('/books', methods=['POST'])
def post_book():
    content = request.get_json()
    print(content)
    connection = psycopg.connect(
        host='localhost',
        dbname='students',
        user='postgres',
        password='password',
        port='5433'
    )
    isbn = content['ISBNコード']
    book_name = content['書籍名']
    publisher = content['出版社']
    author = str(content['著者']).replace('[', '').replace(']', '')
    
    try:
        sql = '''
        INSERT INTO 書籍 ("ISBNコード", 書籍名, 出版社番号, 出版年)
        VALUES (%(isbn)s, %(book_name)s, %(publisher)s, None);
        INSERT INTO 書籍著者 ("ISBNコード", 著者番号, 役割)
        VALUES (%(isbn)s, %(author)s, '著者')
        '''
        connection.execute(sql, {'isbn': isbn, 'book_name':book_name, 'publisher':publisher, 'author':author})
    except Exception:
        connection.rollback()
    else:
        connection.commit()
    return jsonify({'message': 'created'})

# datetime.date() 形式の文字列を YYYY-MM-DD 形式に変換する関数
def convert_date(date_str):
    date_pattern = re.compile(r'datetime\.date\((\d+), (\d+), (\d+)\)')
    match = date_pattern.match(date_str)
    if match:
        year, month, day = match.groups()
        return f'{year}-{month.zfill(2)}-{day.zfill(2)}'
    return date_str
@app.route('/lendings', methods=['GET'])
def get_lendings():

    connection = psycopg.connect(
            host='localhost',
            dbname='book',
            user='postgres',
            password='password',
            port='5433'
        )
    sql = '''
    SELECT * FROM 貸出 JOIN 貸出明細 ON 貸出.貸出番号 = 貸出明細.貸出番号
    JOIN 書籍 ON 貸出明細."ISBNコード" = 書籍."ISBNコード"
    JOIN 書籍著者 ON 書籍."ISBNコード" = 書籍著者."ISBNコード"
    JOIN 著者 ON 書籍著者.著者番号 = 著者.著者番号
    JOIN 出版社 ON 書籍.出版社番号 = 出版社.出版社番号
    JOIN 学生 ON 学生.学生証番号 = 貸出.学生証番号;
    '''
    result = connection.execute(sql)
    rentals = {}
    books = {}

    for entry in result:
        # print(entry)
        # datetime.date() の文字列を処理
        fields = [convert_date(str(field)) for field in entry]
        
        lental_num = fields[0]
        if fields[3] == "None": fields[3] = None
        rental = rentals.setdefault(lental_num, {
            '貸出番号': fields[0],
            '貸出日': fields[1],
            '返却予定日': fields[2],
            '返却確認日': fields[3],
            '書籍': [],
            '学生': {'学生証番号': fields[19], '学生氏名': fields[20]}
        })
        
        book = books.setdefault(fields[7], {
            'ISBNコード': fields[7],
            '書籍名': fields[9],
            '著者': [],
            '出版社': {
                '出版社番号': fields[17],
                '出版社名': fields[18]
            },
            '出版年': fields[11]
        })
        
        if {
            '著者番号': fields[15],
            '著者名': fields[16],
            '役割': fields[14]
        } not in book['著者']:
            book['著者'].append({
                '著者番号': fields[15],
                '著者名': fields[16],
                '役割': fields[14]
            })
        
        if book not in rental['書籍']:
            rental['書籍'].append(book)

    rentals_json = list(rentals.values())
    return jsonify(rentals_json)


@app.route('/lendings', methods=['POST'])
def post_lending():
    content = request.get_json()
    print(content)
    connection = psycopg.connect(
        host='localhost',
        dbname='students',
        user='postgres',
        password='password',
        port='5433'
    )
    target = content
    lend_date = content['貸出日']
    return_d = content['返却予定日']
    s_num = content['学生']
    title = content['書籍'][0]

    sql = '''
    SELECT MAX(貸出番号) FROM 貸出;
    '''
    result = connection.execute(sql)
    for row in result:
        print(row)
        r = str(row).replace('(', '').replace(')', '').replace('\'', '').replace(' ', '').split(',')
        num = int(r[0])+1
    try:
        sql = '''
        INSERT INTO 貸出 (貸出番号, 貸出日, 返却予定日, 返却確認日, 学生証番号)
        VALUES (%(num)s, %(lend_date)s, %(return_d)s, None, %(s_num)s);
        INSERT INTO 貸出明細 (貸出番号, 貸出連番, ISBNコード)
        VALUES (%(num)s, 1, %(title)s);
        '''
        print(sql, lend_date, return_d, s_num, title)
        connection.execute(sql, {'num':num, 'lend_date':lend_date, 'return_d': return_d, 's_num':s_num, 'title':title})
    except Exception:
        connection.rollback()
    else:
        connection.commit()
    return jsonify({'message': 'created'})


@app.route('/lendings/<int:lending_id>/return', methods=['POST'])
def post_lending_return(lending_id):
    content = request.get_json()
    print(content)
    connection = psycopg.connect(
        host='localhost',
        dbname='students',
        user='postgres',
        password='password',
        port='5433'
    )
    target = 43
    date = str(datetime.datetime.now()[:10])
    try:
        sql = '''
        UPDATE 貸出 SET 返却確認日=%(date)s WHERE 貸出番号=%(target)s;
        '''
        print(sql)
        connection.execute(sql, {'date':date, 'target': target})
    except Exception:
        connection.rollback()
    else:
        connection.commit()
    return jsonify({'message': 'created'})


@app.route('/users', methods=['GET'])
def get_users():
    users = [
        {'学生証番号': '5420365', '学生氏名': '矢吹紫'},
        {'学生証番号': '5419513', '学生氏名': '高橋博之'},
        {'学生証番号': '6121M78', '学生氏名': '田中淳平'}
    ]
    connection = psycopg.connect(
        host='localhost',
        dbname='book',
        user='postgres',
        password='password',
        client_encoding='utf-8',
        port='5433'
    )

    sql = '''
    SELECT * FROM 学生;
    '''
    result = connection.execute(sql)
    return jsonify(users)


@app.route('/users', methods=['POST'])
def post_users():
    content = request.get_json()
    print(content)
    connection = psycopg.connect(
    host='localhost',
    dbname='students',
    user='postgres',
    password='password',
    port='5433'
    )
    s_id = content['学生証番号']
    s_name = content['学生氏名']
    
    print(s_id, s_name)
    try:
        sql = '''
        INSERT INTO 学生 (学生証番号, 学生氏名) VALUES (%(s_id)s, %(s_name)s);
        '''
        print(sql)
        connection.execute(sql, {'s_id':s_id, 's_name': s_name})
    except Exception:
        connection.rollback()
    else:
        connection.commit()
    return jsonify({'message': 'created'})
