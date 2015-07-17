''' pybq.util: basic api client utility functions

some modeled after bigquery documentation
- https://cloud.google.com/bigquery/docs/managing_jobs_datasets_projects
- https://cloud.google.com/bigquery/docs/tables
'''


import sys
import time
import datetime
import os
sys.path.append('/Users/amyskerry/google-cloud-sdk/platform/bq')
import cfg


def _log_query(client, query_response):
    if client.logging_file is not None:
        query = query_response['configuration']['query']['query']
        destination = query_response['configuration'][
            'query']['destinationTable']
        date = str(datetime.datetime.fromtimestamp(time.time()))
        usage_stats = query_response['statistics']
        cached = str(usage_stats['query']['cacheHit'])
        jobid = query_response['id']
        user = query_response['user_email']
        duration_ms = str(
            (float(usage_stats['endTime']) - float(usage_stats['creationTime'])) / 1000)
        processed_mb = str(float(usage_stats['totalBytesProcessed']) / 1000000)
        if not os.path.exists(client.logging_file):
            header = '|'.join(
                ['date', 'user', 'query', 'destination', 'jobid', 'duration_ms', 'processed_mb', 'cached', '\n'])
            with open(client.logging_file, 'a') as f:
                f.write(header)
        logline = '|'.join(
            [date, user, query, stringify(destination), jobid, duration_ms, processed_mb, cached, '\n'])
        with open(client.logging_file, 'a') as f:
            f.write(logline)



class Mask_Printing():

    '''
    Defines a context manager for masking printing of all functions within the context scope

    USAGE:
        with Mask_Printing():
            func_that_prints_stuff() #output won't be printed to stdout
        other_func_that_prints_stuff() #normal printing resumed
    '''

    def __init__(self):
        class NullWriter():

            def write(self, arg):
                pass

            def flush(self):
                pass

            encoding = 'ascii'
        self.nullwrite = NullWriter()
        self.proper_stdout = sys.stdout

    def __enter__(self):
        '''when entering the context, change sys.stdout to print nothing'''
        if not cfg.DEBUG:
            sys.stdout = self.nullwrite

    def __exit__(self, type, value, traceback):
        '''when exiting the context, restore default stdout'''
        if not cfg.DEBUG:
            sys.stdout = self.proper_stdout


def stringify(tabledict):
    '''converts a table dictionary () to bq-style path string'''
    return "%s:%s.%s" % (tabledict['projectId'], tabledict['datasetId'], tabledict['tableId'])

# define a mapping to use in various conversions
mapping = {'text': "STRING", 'char': "STRING", 'varchar': "STRING", 'int': "INTEGER", 'tinyint': "INTEGER", 'smallint': "INTEGER", 'mediumint': "INTEGER",
           'bigint': "INTEGER", 'float': "FLOAT", 'double': "FLOAT", 'decimal': "FLOAT", 'bool': "BOOLEAN", 'date': "TIMESTAMP", 'datetime': "TIMESTAMP"}


def bqjson_from_sql_schema(cursor, tablename):
    '''accesses sql table schema and returns corresponding json for defining bq schema'''
    import json
    schema = query(cursor, 'describe %s' % tablename)
    dicts = []
    for line in schema:
        datatype = mapping[
            ''.join([char for char in line[1].lower() if char.isalpha()])]
        mode = False
        if line[2] == 'NO':
            mode = "REQUIRED"
        d = {"name": line[0], 'type': datatype}
        if mode:
            d['mode'] = mode
        dicts.append(d)
    return json.dumps(dicts)


def bqjson_from_csv(con, csvpath):
    '''returns bq-style json schema for the provided csv (requires db connection because it uses a temporary sql table)'''
    import json
    string = create_table_string(csvpath, con, 'temporary_table')
    s = string[string.index(
        '(') + 2:-len(' ENGINE=InnoDB DEFAULT CHARSET=utf8') - 2]
    dicts = []
    for line in s.split('\n')[1:]:
        line = line.strip().split(' ')
        datatype = mapping[
            ''.join([char for char in line[1].lower() if char.isalpha()])]
        mode = "REQUIRED"
        if len(line) > 2 and line[3] == 'NULL':
            mode = False
        d = {"name": line[0], 'type': datatype}
        if mode:
            d['mode'] = mode
        dicts.append(d)
    return json.dumps(dicts)


def query(cursor, query):
    '''query sql cursor'''
    cursor.execute(query)
    return cursor.fetchall()


def local_sql_connect(dbname, host='localhost', username='root'):
    '''create connection to local mysql server'''
    con = MySQLdb.connect(host, username, sqlcfg.passwd, dbname)
    cursor = con.cursor()
    return con, cursor


def connect_cloudsql(cloudname, cloudip):
    '''create connection to remote google cloudsql instance'''
    env = os.getenv('SERVER_SOFTWARE')
    if (env and env.startswith('Google App Engine/')):
        # Connecting from App Engine
        db = MySQLdb.connect(
            unix_socket='/cloudsql/%s:sql' % cloudname,
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
