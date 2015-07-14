''' pybq.util: basic api client utility functions

modeled after bigquery documentation
- https://cloud.google.com/bigquery/docs/managing_jobs_datasets_projects
- https://cloud.google.com/bigquery/docs/tables
'''


import sys
sys.path.append('/Users/amyskerry/google-cloud-sdk/platform/bq')
from bq import apiclient
from apiclient import googleapiclient, oauth2client
import googleapiclient.discovery
import oauth2client.client
from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials
import pprint


def get_service():
    """returns an initialized and authorized bigquery client"""
    credentials = GoogleCredentials.get_application_default()
    if credentials.create_scoped_required():
        credentials = credentials.create_scoped(
            'https://www.googleapis.com/auth/bigquery')
    return build('bigquery', 'v2', credentials=credentials)


def list_projects(service):
    """list projects associated with the service

    INPUTS:
        service (str): serviceid requested
    OUTPUTS:
        projectids (list): list of projectids associated with the service
    """
    try:
        projects = service.projects()
        list_reply = projects.list().execute()
        projectids = []
        if 'projects' in list_reply:
            print 'Project list:'
            projects = list_reply['projects']
            for p in projects:
                projectids.append(p['id'])
                print "%s: %s" % (p['friendlyName'], p['id'])
        else:
            print "No projects found."
        return projectids
    except apiclient.errors.HttpError as err:
        print 'Error in list_projects:', pprint.pprint(err.content)


def list_datasets(service, project):
    """list datasets associated with the project

    INPUTS:
        service (str): serviceid requested
        project (str): projectid requested
    OUTPUTS:
        datasetids (list): list of datasetids associated with the project
    """
    try:
        datasets = service.datasets()
        list_reply = datasets.list(projectId=project).execute()
        datasetids = []
        if 'datasets' in list_reply:

            print 'Dataset list:'
            datasets = list_reply['datasets']
            for d in datasets:
                print d['datasetReference']['datasetId']
                datasetids.append(d['datasetReference']['datasetId'])
        else:
            print "No datasets found."
        return datasetids
    except apiclient.errors.HttpError as err:
        print 'Error in list_datasets:', pprint.pprint(err.content)


def list_tables(service, project, dataset):
    """list tables associated with the dataset

    INPUTS:
        service (str): serviceid requested
        project (str): projectid requested
        dataset (str): datasetid requested
    OUTPUTS:
        tableids (list): list of tableids associated with the project
    """
    try:
        tables = service.tables()
        list_reply = tables.list(
            projectId=project, datasetId=dataset).execute()
        tableids = []
        if 'tables' in list_reply:
            print 'Tables list:'
            tables = list_reply['tables']
            for t in tables:
                print t['tableReference']['tableId']
                tableids.append(t['tableReference']['tableId'])
        else:
            print "No tables found."
        return tableids
    except apiclient.errors.HttpError as err:
        print 'Error in list_tables:', pprint.pprint(err.content)


def delete_table(service, projectid, datasetid, tableid):
    """deletes specified table"""
    service.tables().delete(projectId=projectid,
                            datasetId=datasetid,
                            tableId=tableid).execute()


def copy_table(service, source=None, target=None):
    """copies table from source to target

    INPUTS:
        source (dict): dictionary specifying cp source (keys: 'projectId', 'datasetId', 'tableId')
        target (dict): dictionary specifying cp target (keys: 'projectId', 'datasetId', 'tableId')
    """

        jobCollection = service.jobs()
        jobData = {
            "projectId": sourceProjectId,
            "configuration": {
                "copy": {
                    "sourceTable": {
                        "projectId": source['projectId'],
                        "datasetId": source['datasetId'],
                        "tableId": source['tableId'],
                    },
                    "destinationTable": {
                        "projectId": target['projectId'],
                        "datasetId": target['datasetId'],
                        "tableId": target['tableId'],
                    },
                    "createDisposition": "CREATE_IF_NEEDED",
                    "writeDisposition": "WRITE_TRUNCATE"
                }
            }
        }

        insertResponse = jobCollection.insert(
            projectId=targetProjectId, body=jobData).execute()
        # Ping for status until it is done, with a short pause between calls.
        import time
        while True:
            status = jobCollection.get(projectId=target['projectId'],
                                       jobId=insertResponse['jobReference']['jobId']).execute()
            if 'DONE' == status['status']['state']:
                break
            print 'Waiting for the import to complete...'
            time.sleep(10)
        if 'errors' in status['status']:
            print 'Error loading table: ', pprint.pprint(status)
            return
        print 'Loaded the table:', pprint.pprint(status)  # !!!!!!!!!!
        # Now query and print out the generated results table.
        queryTableData(
            service, target['projectid'], target['datasetid'], target['tableid'])

    except apiclient.errors.HttpError as err:
        print 'Error in loadTable: ', pprint.pprint(err.resp)


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
        self.nullwrite = NullWriter()
        self.proper_stdout = sys.stdout

    def __enter__(self):
        '''when entering the context, change sys.stdout to print nothing'''
        sys.stdout = self.nullwrite

    def __exit__(self, type, value, traceback):
        '''when exiting the context, restore default stdout'''
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
