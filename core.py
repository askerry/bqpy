''' pybq.core: core functions for querying and fetching data from bigquery
'''


import pandas as pd
import numpy as np
import sys
import os
import datetime
import time
import json
from pympler.asizeof import asizeof
import cfg
sys.path.append(cfg.gsdk_path)
import bq
import matplotlib.pyplot as plt
import seaborn as sns
sns.set_style('white')
import util
from bq import apiclient
from apiclient import googleapiclient, oauth2client
import googleapiclient.discovery
import oauth2client.client
from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials
import pprint


# TODOs
# - fix fact that all of this uses an arbitrary/weird mashup of python client and json api
# - support exporting from cloud storage/datastore to bigquery
# - making caching/logging less janky


def run_query(con, querystr, destination_table=None, dry_run=False, create_disposition='CREATE_IF_NEEDED', write_disposition='WRITE_EMPTY', allow_large_results=False):
    '''executes a query on remote bq dataset

    INPUTS:
        con (pybq.core.Connection) for interfacing with remote project
            includes client (bq.Client): connected client for querying database
        querystr (str): sql string specifying the query
        destination_table (str): bq-style path to table ('projectid:datasetid.tableid') for writing results of query.
            if destination is None, client will create temporary table.
        dry_run (bool): if True, query won't actually execute but just return stats on query
    OUPUTS:
        query_response (dict): dictionary summarizing action taken by query,
            including remote destination table that contains the query results
    '''
    if destination_table is not None:
        print "writing to temporary table %s" % destination_table
    query_response = con.client.Query(
        querystr, destination_table=destination_table, dry_run=dry_run, write_disposition=write_disposition, create_disposition=create_disposition, allow_large_results=allow_large_results)
    util._log_query(con, query_response)
    return query_response


def fetch_query(con, query_response, start_row=0, max_rows=cfg.MAX_ROWS):
    '''fetches the results specified in the query response and returns them locally

    INPUTS:
        con (pybq.core.Connection) for interfacing with remote project
            includes client (bq.Client): connected client for querying database
        query_response (dict): dictionary specifying query_response (output of run_query),
            including remote table containing the query results
        start_row (int): row to start at when returning results
        max_rows (int): max # of rows to fetch
    OUPUTS:
        fields (list): list of dictionaries representing table schema with names and types for each column
        data (list): list of rows of data from table resulting from query
    '''
    fetch_table = query_response['configuration']['query']['destinationTable']
    fields, data = con.client.ReadSchemaAndRows(
        fetch_table, start_row=start_row, max_rows=max_rows)
    return fields, data


def bqresult_2_df(fields, data):
    # TODO make type conversion/ schema inference less hacky/more robust
    '''takes the output of the bigquery call and returns data as a dataframe with appropriate dtypes
    INPUTS:
        fields (list): list of dictionaries representing table schema with names and types for each column
        data (list): list of rows of data from table resulting from query
    OUPUTS:
        df (pandas dataframe): dataframe with fields as columns and data as rows
    '''
    format_table = {'STRING': unicode, 'FLOAT': np.float64,
                    'INTEGER': np.int64, 'DATE': 'datetime64[ns]', 'TIMESTAMP': 'datetime64[ns]', 'BOOLEAN': bool}
    cols = [field['name'] for field in fields]
    dtypes = {field['name']: format_table[field['type']] for field in fields}
    df = pd.DataFrame(columns=cols, data=data)
    df = df.fillna(np.nan)
    for key, value in dtypes.items():
        if "date" in key or 'time' in key:
            df[key] = df[key].astype('datetime64')
        else:
            try:
                df[key] = df[key].astype(value)
            except ValueError:
                df[key] = df[key].astype(np.float64)

    return df





def create_column_from_values(con, col, content, remotetable, length=None):
    '''create new dataframe with column content (which can then be joined with existing table)'''
    d = util.dictify(remotetable)
    d['tableId'] = d['tableId'] + '_newcol_' + \
        str(np.random.randint(1000, 10000))
    if not hasattr(content, '__iter__'):
        try:
            np.isnan(content)
        except TypeError:
            content = unicode(content)
        content = [content for i in range(length)]
    df = pd.DataFrame({col: content})
    con, dest = write_df_to_remote(
        con, df, overwrite_method='fail', projectId=d['projectId'], datasetId=d['datasetId'], tableId=d['tableId'])
    return dest

#option one: streaming insert -- but note there are various constraints:
#Maximum row size: 20 KB
#Maximum data size of all rows, per insert: 1 MB
#Maximum rows per second: 10,000 rows per second, per table
#Maximum rows per request: 500
#Maximum bytes per second: 10 MB per second, per table.
#Streaming: $0.01 per 100K rows streamed (after 01.01.2015)


def stream_df_to_remote(con, df, overwrite_method='fail', projectId=None, datasetId=None, tableId=None):
    '''write pandas dataframe as bigquery table'''
    schema = {"fields": util.bqjson_from_df(df, dumpjson=False)}
    dataset_ref = {'datasetId': datasetId,
                   'projectId': projectId}
    table_ref = {'tableId': tableId,
                 'datasetId': datasetId,
                 'projectId': projectId}
    table = {"kind": "bigquery#table",
             'tableReference': table_ref, 'schema': schema}
    try:
        con.client._apiclient.tables().insert(
            body=table, **dataset_ref).execute()
    except:
        pass
    datarows = []
    for i, row in df.iterrows():
        jsondata = {col: row[col] for col in df.columns}
        datarows.append({"json": jsondata})
    
    body = {'kind': 'bigquery#tableDataInsertAllRequest', 'rows': datarows}
    update = con.client._apiclient.tabledata().insertAll(
        body=body, **table_ref).execute()
    return con, util.stringify(table_ref)

def write_df_to_remote(con, df, overwrite_method='fail',delete='True', name=None, projectId=None, datasetId=None, tableId=None):
    '''write pandas dataframe as bigquery table'''
    schema = {"fields": util.bqjson_from_df(df, dumpjson=False)}
    table_ref = {'tableId': tableId,
                 'datasetId': datasetId,
                 'projectId': projectId}
    if overwrite_method == 'append':
        write_disposition = 'WRITE_APPEND'
    elif overwrite_method == 'overwrite':
        write_disposition = 'WRITE_TRUNCATE'
    else:
        write_disposition = 'WRITE_EMPTY'
    df.to_csv(tableId+'.csv', index=False)
    filename=os.path.join(os.getcwd(), tableId+'.csv')
    project=util.dictify(self.remote)['projectId']
    if name is None:
        name= datasetId+tableId
    util.file_to_bucket(con.client._storageclient, project, self.bucket, filename, name=name)
    loadjob = {'destinationTable':table_ref, 'schema':schema, 'writeDisposition':write_disposition, 'sourceUris':['gs://%s/%s' %(bucket, name)], 'skipLeadingRows':1}
    jobbody = {'configuration':{'load':loadjob}}
    updatejob= con.client._apiclient.jobs().insert(projectId=projectId, body=jobbody).execute()
    status='PENDING'
    while status != 'DONE':
        status = con.client._apiclient.jobs().get(**updatejob['jobReference']).execute()['status']['state']
    if delete:
        delete_from_bucket(con.client._storageclientt, project, bucket, name)
    return con, util.stringify(table_ref)


class Connection(object):

    '''connects to a bigquery client for the provided project_id

    INPUTS:
        project_id (str): project_id from google app developer console
    OUPUTS:
        connection (object): class for interfacing with remote bigquery project
            includes connection.client (bq.Client) provides connected client for querying database
    '''

    def __init__(self, project_id=None, logging_file=None, cache_max=cfg.CACHE_MAX):
        self.project_id = project_id
        self.logging_file = logging_file
        self.querycache = {}
        self.cache_max = cache_max
        if cache_max is None:
            self.cache_max = cfg.CACHE_MAX
        self.client = bq.Client.Get()
        self.client.project_id = project_id
        self.client._apiclient = util.get_bq_service()
        self.client._storageclient = util.get_storage_service()

    def create_table(self, project_id, dataset_id, table_id, df, df_obj):
        _, table = write_df_to_remote(
            self, df, overwrite_method='fail', projectId=project_id, datasetId=dataset_id, tableId=table_id)
        return df_obj(self, table)

    def view_log(self):
        return pd.read_csv(self.logging_file, delimiter='|')

    def update(self):
        self.flush_cache()

    def _cache_query(self, querystr, df, source, fetch):
        if self.cache_max > 0:
            while float(asizeof(self.querycache)) / 1048576 >= self.cache_max:
                del self.querycache[self.querycache.keys()[0]]
            self.querycache[querystr] = {}
            self.querycache[querystr]['local'] = df
            self.querycache[querystr]['source'] = source
            self.querycache[querystr]['fetched'] = fetch
            self.querycache[querystr]['timestamp'] = time.time()

    def flush_cache(self):
        self.querycache = {}

    # Broken and low priority
    '''
    def storage_report(self, projects=None, datasets=None, tables=None, storage_price_GBDay=.20 / 30):
        storage_data = {'project': [], 'dataset': [], 'table': [],
                        'storage': [], 'duration': [], 'price': [], 'cost': []}
        if projects is None:
            projects = [
                p['id'] for p in self.client._apiclient.projects().list().execute()['projects']]
        for p in projects:
            all_sets = [d['id'] for d in self.client._apiclient.datasets().list(
                projectId=p).execute()['datasets']]
            if datasets is None:
                datasets = all_sets
            for d in all_sets:
                if d['datasetReference']['datasetId'] in datasets:
                    all_tables = [t['id'] for t in self.client._apiclient.datasets().list(
                        projectId=p, datasetId=d).execute()['tables']]
                    if tables is None:
                        tables = all_tables
                    for t in all_tables:
                        if d['tableReference']['tableId']in tables:
                            resource = get_table_resource(
                                self.service, {'projectId': p, 'datasetId': d, 'tableId': t})
                            existed_dur = datetime.datetme.now(
                            ) - util.convert_timestamp(resource['creationTime'])
                            days = float(existed_dur) / 60 / 60 / 24
                            storage_data['project'].append(p)
                            storage_data['dataset'].append(d)
                            storage_data['table'].append(t)
                            mb = resource['numBytes'] / 1048576
                            storage_data['MB'].append(mb)
                            storage_data['duration'].append(months)
                            storage_data[
                                'price(GB/day)'].append(storage_price_GBDay)
                            storage_data['cost'].append(
                                days * (mb / 1000) * storage_price_GBDay)
        return pd.DataFrame(data=storage_data)
    '''

    def _check_query(self, querystr, fetch, last_modified):
        return querystr in self.querycache and self.querycache[querystr]['fetched'] == fetch and self.querycache[querystr]['timestamp'] > last_modified

    def _fetch_from_cache(self, querystr):
        print "Fetching from local cache."
        return self.querycache[querystr]['local'], self.querycache[querystr]['source'], False

    def list_all_projects(self):
        """list projects associated with the service

        INPUTS:
            service (str): serviceid requested
        OUTPUTS:
            projectids (list): list of projectids associated with the service
        """
        try:
            projects = self.client._apiclient.projects()
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
            print "\n"
            return projectids
        except apiclient.errors.HttpError as err:
            print 'Error in list_projects:', pprint.pprint(err.content)

    def list_datasets(self, project):
        """list datasets associated with the project

        INPUTS:
            service (str): serviceid requested
            project (str): projectid requested
        OUTPUTS:
            datasetids (list): list of datasetids associated with the project
        """
        try:
            datasets = self.client._apiclient.datasets()
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
            print "\n"
            return datasetids
        except apiclient.errors.HttpError as err:
            print 'Error in list_datasets:', pprint.pprint(err.content)

    def list_tables(self, project, dataset):
        """list tables associated with the dataset

        INPUTS:
            service (str): serviceid requested
            project (str): projectid requested
            dataset (str): datasetid requested
        OUTPUTS:
            tableids (list): list of tableids associated with the project
        """
        try:
            tables = self.client._apiclient.tables()
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
            print "\n"
            return tableids
        except apiclient.errors.HttpError as err:
            print 'Error in list_tables:', pprint.pprint(err.content)

    def delete_table(self, projectid, datasetid, tableid):
        """deletes specified table"""
        self.client._apiclient.tables().delete(projectId=projectid,
                                               datasetId=datasetid,
                                               tableId=tableid).execute()

    def create_table(self, projectid, datasetit, tableid, schema):
        dataset_ref = {'datasetId': dataset_id,
                       'projectId': project_id}
        table_ref = {'tableId': table_id,
                     'datasetId': dataset_id,
                     'projectId': project_id}
        table = {"kind": "bigquery#table",
                 'tableReference': table_ref, 'schema': {'fields': schema}}
        con.client._apiclient.tables().insert(
            body=table, **dataset_ref).execute()
