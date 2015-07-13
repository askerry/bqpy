import sys
sys.path.append('/Users/amyskerry/google-cloud-sdk/platform/bq')
from bq import apiclient
from apiclient import googleapiclient, oauth2client
import googleapiclient.discovery
import oauth2client.client
from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials
import pprint

# basic api client utility functions (modeled after
# https://cloud.google.com/bigquery/docs/managing_jobs_datasets_projects
# and https://cloud.google.com/bigquery/docs/tables)


def get_service():
    """returns an initialized and authorized bigquery client"""

    credentials = GoogleCredentials.get_application_default()
    if credentials.create_scoped_required():
        credentials = credentials.create_scoped(
            'https://www.googleapis.com/auth/bigquery')
    return build('bigquery', 'v2', credentials=credentials)


def list_projects(service):
    try:
        # Start training on a data set
        projects = service.projects()
        list_reply = projects.list().execute()
        if 'projects' in list_reply:
            print 'Project list:'
            projects = list_reply['projects']
            for p in projects:
                print "%s: %s" % (p['friendlyName'], p['id'])
        else:
            print "No projects found."

    except apiclient.errors.HttpError as err:
        print 'Error in ListProjects:', pprint.pprint(err.content)


def list_datasets(service, project):
    try:
        datasets = service.datasets()
        list_reply = datasets.list(projectId=project).execute()
        if 'datasets' in list_reply:

            print 'Dataset list:'
            datasets = list_reply['datasets']
            for d in datasets:
                print d['datasetReference']['datasetId']
        else:
            print "No datasets found."

    except apiclient.errors.HttpError as err:
        print 'Error in ListDatasets:', pprint.pprint(err.content)


def list_tables(service, project, dataset):
    try:
        tables = service.tables()
        list_reply = tables.list(
            projectId=project, datasetId=dataset).execute()
        if 'tables' in list_reply:
            print 'Tables list:'
            tables = list_reply['tables']
            for t in tables:
                print t['tableReference']['tableId']
        else:
            print "No tables found."

    except apiclient.errors.HttpError as err:
        print 'Error in listTables:', pprint.pprint(err.content)


def delete_table(service, projectId, datasetId, tableId):
    service.tables().delete(projectId=projectId,
                            datasetId=datasetId,
                            tableId=tableId).execute()


def copy_table(service):
    try:
        sourceProjectId = raw_input("What is your source project? ")
        sourceDatasetId = raw_input("What is your source dataset? ")
        sourceTableId = raw_input("What is your source table? ")

        targetProjectId = raw_input("What is your target project? ")
        targetDatasetId = raw_input("What is your target dataset? ")
        targetTableId = raw_input("What is your target table? ")

        jobCollection = service.jobs()
        jobData = {
            "projectId": sourceProjectId,
            "configuration": {
                "copy": {
                    "sourceTable": {
                        "projectId": sourceProjectId,
                        "datasetId": sourceDatasetId,
                        "tableId": sourceTableId,
                    },
                    "destinationTable": {
                        "projectId": targetProjectId,
                        "datasetId": targetDatasetId,
                        "tableId": targetTableId,
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
            status = jobCollection.get(projectId=targetProjectId,
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
            service, targetProjectId, targetDatasetId, targetTableId)

    except apiclient.errors.HttpError as err:
        print 'Error in loadTable: ', pprint.pprint(err.resp)


# context manager for masking printing
# e.g. with Mask_Printing():
#           func_that_prints_stuff()

class Mask_Printing():

    def __init__(self):
        class NullWriter():

            def write(self, arg):
                pass

            def flush(self):
                pass
        self.nullwrite = NullWriter()
        self.proper_stdout = sys.stdout

    def __enter__(self):
        sys.stdout = self.nullwrite

    def __exit__(self, type, value, traceback):
        sys.stdout = self.proper_stdout


def stringify(destinationtable):
    return "%s:%s.%s" % (destinationtable['projectId'], destinationtable['datasetId'], destinationtable['tableId'])


mapping = {'text': "STRING", 'char': "STRING", 'varchar': "STRING", 'int': "INTEGER", 'tinyint': "INTEGER", 'smallint': "INTEGER", 'mediumint': "INTEGER",
           'bigint': "INTEGER", 'float': "FLOAT", 'double': "FLOAT", 'decimal': "FLOAT", 'bool': "BOOLEAN", 'date': "TIMESTAMP", 'datetime': "TIMESTAMP"}


def bqjson_from_sql_schema(cursor, tablename):
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
    cursor.execute(query)
    return cursor.fetchall()


def local_sql_connect(dbname):
    con = MySQLdb.connect('localhost', 'root', sqlcfg.passwd, dbname)
    cursor = con.cursor()
    return con, cursor


def connect_cloudsql(cloudname, cloudip):
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
