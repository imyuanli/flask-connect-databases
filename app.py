from flask import Flask
from flask import jsonify
from flask import request
from flask_sqlalchemy import SQLAlchemy
from impala.dbapi import connect

app = Flask(__name__)
db = SQLAlchemy(app)  # 创建一个对象，设置名为db
# 关闭数据库修改跟踪操作[提高性能]，可以设置为True，这样可以跟踪操作：
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
# 开启输出底层执行的sql语句
app.config['SQLALCHEMY_ECHO'] = True
# 开启数据库的自动提交功能[一般不使用]
app.config['SQLALCHEMY_COMMIT_ON_TEARDOWN'] = True


def connect_db(info, node_type):
    type_dict = {
        "Oracle": "oracle",
        "MySQL": "mysql+pymysql",
    }
    # node 信息
    driver = type_dict.get(node_type)
    username = info.get('username')
    password = info.get('password')
    host = info.get('host')
    port = info.get('port')
    database = info.get('database')

    # 不同数据库的连接方法
    if node_type == 'Oracle' or node_type == 'MySQL':
        app.config['SQLALCHEMY_DATABASE_URI'] = f"{driver}://{username}:{password}@{host}:{port}/{database}"
        engine = db.get_engine()
        conn = engine.connect()
        conn.close()
        return engine
    elif node_type == "Hive":
        conn = connect(host=host,
                       port=port,
                       auth_mechanism='PLAIN',
                       user=username,
                       password=password,
                       database=database)
        cur = conn.cursor()
        cur.execute('SHOW TABLES')
        return cur


@app.route('/test_db_connect', methods=['POST'])
def test_db_connect():
    res = request.json
    data_node = res['dataNode']
    node_type = res['type']
    info = data_node[node_type]
    connect_db(info, node_type)
    return jsonify({"message": "success"})


@app.route('/get_tables', methods=['POST'])
def get_tables():
    res = request.json
    dataNode = res['dataNode']
    node_type = dataNode.get('type')
    tables = ""
    result = connect_db(dataNode, node_type)
    obj = {}
    if node_type == 'Oracle' or node_type == 'MySQL':
        # 获取所有表
        tables = result.table_names()
    elif node_type == "Hive":
        # 获取所有表
        res = result.fetchall()
        arr = []
        for i in res:
            arr.append(i[0])
        tables = arr
    return jsonify({
        "code": 200,
        "data": tables
    })


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)
