import json

import demjson
from flask import Flask
from flask import jsonify
from flask import request
from flask_sqlalchemy import SQLAlchemy
from impala.dbapi import connect
from sqlalchemy import create_engine, inspect, MetaData, Table
from sqlalchemy.orm import sessionmaker

app = Flask(__name__)

def connect_db(info, node_type):
    type_dict = {
        "Oracle": "oracle",
        "MySQL": "mysql+pymysql",
        "Hive": "impala",
    }
    # node 信息
    driver = type_dict.get(node_type)
    username = info.get('username')
    password = info.get('password')
    host = info.get('host')
    port = info.get('port')
    database = info.get('database')
    # 连接方式
    db_url = f"{driver}://{username}:{password}@{host}:{port}/{database}"
    if node_type == "Hive":
        db_url += '?auth_mechanism=PLAIN'
    engine = create_engine(db_url)
    return engine


def ok(data):
    return {"code": 200, "data": data}


@app.route('/test_db_connect', methods=['POST'])
def test_db_connect():
    res = request.json
    data_node = res['dataNode']
    node_type = res['type']
    info = data_node[node_type]
    connect_db(info, node_type)
    return jsonify(ok(""))


@app.route('/get_tables', methods=['POST'])
def get_tables():
    res = request.json
    dataNode = res['dataNode']
    node_type = dataNode.get('type')
    engine = connect_db(dataNode, node_type)
    insp = inspect(engine)
    tables = insp.get_table_names()
    return jsonify(ok(tables))


@app.route('/get_columns_info', methods=['POST'])
def get_columns_info():
    res = request.json
    data = res['item']
    table = data.get('table')
    data_node = data.get('dataNode')
    node_type = data_node.get('type')
    info = data_node.get(node_type)
    # 创建连接
    engine = connect_db(info, node_type)
    insp = inspect(engine)
    if insp:
        columns = insp.get_columns(table)
        meta = MetaData()
        table = Table(table, meta, autoload=True, autoload_with=engine)
        primaryKeyColNames = [pk_column.name for pk_column in table.primary_key.columns.values()]
        # 处理数据
        if len(primaryKeyColNames) <= 1:
            key = ""
            if len(primaryKeyColNames) == 1:
                key = primaryKeyColNames[0]
            for column in columns:
                column['type'] = str(column['type'])
                if column['name'] == key:
                    column['isPrimaryKey'] = 'true'
                else:
                    column['isPrimaryKey'] = 'false'
        else:
            for key in primaryKeyColNames:
                for column in columns:
                    column['type'] = str(column['type'])
                    if column['name'] == key:
                        column['isPrimaryKey'] = 'true'
                    else:
                        column['isPrimaryKey'] = 'false'
        return jsonify(ok(columns))


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)
