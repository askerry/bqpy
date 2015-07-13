import MySQLdb
import os
import sys
sys.path.append('/users/amyskerry/documents/projects')
from credentials import sqlcfg


mapping= {'text':"STRING",'char':"STRING",'varchar':"STRING",'int':"INTEGER", 'tinyint':"INTEGER",'smallint':"INTEGER",'mediumint':"INTEGER",'bigint':"INTEGER",'float':"FLOAT",'double':"FLOAT",'decimal':"FLOAT",'bool':"BOOLEAN",'date':"TIMESTAMP",'datetime':"TIMESTAMP"}


def query(cursor, query):
    cursor.execute(query)
    return cursor.fetchall()

def local_sql_connect(dbname):
    con=MySQLdb.connect('localhost', 'root', sqlcfg.passwd, dbname)
    cursor=con.cursor()
    return con, cursor


def connect_cloudsql(cloudname, cloudip):
    env = os.getenv('SERVER_SOFTWARE')
    if (env and env.startswith('Google App Engine/')):
        # Connecting from App Engine
      db = MySQLdb.connect(
          unix_socket='/cloudsql/%s:sql' %cloudname,
          user='root')
    else:
        # Connecting from an external network.
        # Make sure your network is whitelisted
      db = MySQLdb.connect(
          host=cloudip,
          port=3306,
          user='askerry', passwd=sqlcfg.passwd)

    cursor = db.cursor()
    print query(cursor, 'show databases')
    return cursor

def bqjson_from_sql_schema(cursor, tablename):
    import json
    schema=query(cursor, 'describe %s' %tablename)
    dicts=[]
    for line in schema:
        datatype=mapping[''.join([char for char in line[1].lower() if char.isalpha()])]
        mode=False
        if line[2]=='NO':
            mode="REQUIRED"
        d={"name":line[0], 'type':datatype}
        if mode:
            d['mode']=mode
        dicts.append(d)
    return json.dumps(dicts)


def bqjson_from_csv(con, csvpath):
    import json
    string = create_table_string(csvpath, con, 'temporary_table')
    s = string[string.index('(')+2:-len(' ENGINE=InnoDB DEFAULT CHARSET=utf8')-2]
    dicts = []
    for line in s.split('\n')[1:]:
        line = line.strip().split(' ')
        datatype = mapping[''.join([char for char in line[1].lower() if char.isalpha()])]
        mode = "REQUIRED"
        if len(line)>2 and line[3]=='NULL':
            mode = False
        d = {"name":line[0], 'type':datatype}
        if mode:
            d['mode'] = mode
        dicts.append(d)
    return json.dumps(dicts)


hosts={'eeg':['eeg4kaggle','173.194.235.243'], 'khan':['khan','']}
