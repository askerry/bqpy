''' pybq.core: core functions for querying and fetching data from bigquery
'''


import pandas as pd
import numpy as np
import sys
import os
import datetime
import time
from pympler.asizeof import asizeof
sys.path.append('/Users/amyskerry/google-cloud-sdk/platform/bq')
import bq
import matplotlib.pyplot as plt
import seaborn as sns
sns.set_style('white')
import util
import cfg
from bq import apiclient
from apiclient import googleapiclient, oauth2client
import googleapiclient.discovery
import oauth2client.client
from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials
import pprint


def run_query(con, querystr, destination_table=None, dry_run=False):
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
        querystr, destination_table=destination_table, dry_run=dry_run)
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


def get_service():
    """returns an initialized and authorized bigquery client"""
    credentials = GoogleCredentials.get_application_default()
    if credentials.create_scoped_required():
        credentials = credentials.create_scoped(
            'https://www.googleapis.com/auth/bigquery')
    return build('bigquery', 'v2', credentials=credentials)


class Connection():

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
        self.querylog = {}
        if cache_max is None:
            self.cache_max = cfg.CACHE_MAX
        else:
            self.cache_max = cache_max
        self.client = bq.Client.Get()
        self.client.project_id = project_id
        self.service = get_service()

    def view_log(self):
        return pd.read_csv(self.logging_file, delimiter='|')

    def update(self):
        self.flush_cache()

    def _cache_query(self, querystr, df, source, fetch):
        if self.cache_max > 0:
            i = 0
            print float(asizeof(self.querycache)) / 1048576
            while float(asizeof(self.querycache)) / 1048576 >= self.cache_max:
                del self.querycache[self.querycache.keys()[i]]
                i += 1
            self.querycache[querystr] = {}
            self.querycache[querystr]['local'] = df
            self.querycache[querystr]['source'] = source
            self.querycache[querystr]['fetched'] = fetch

    def _log_query(self, querystr):
        timestamp = str(datetime.datetime.fromtimestamp(time.time()))
        self.querylog[timestamp] = querystr

    def flush_cache(self):
        self.querycache = {}

    def _check_query(self, querystr, fetch):
        return querystr in self.querycache and self.querycache[querystr]['fetched'] == fetch

    def _fetch_from_cache(self, querystr):
        print "fetching from local cache"
        return self.querycache[querystr]['local'], self.querycache[querystr]['source']

    def list_all_projects(self):
        """list projects associated with the service

        INPUTS:
            service (str): serviceid requested
        OUTPUTS:
            projectids (list): list of projectids associated with the service
        """
        try:
            projects = self.service.projects()
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

    def list_datasets(self, project):
        """list datasets associated with the project

        INPUTS:
            service (str): serviceid requested
            project (str): projectid requested
        OUTPUTS:
            datasetids (list): list of datasetids associated with the project
        """
        try:
            datasets = self.service.datasets()
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
            tables = self.service.tables()
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

    def delete_table(projectid, datasetid, tableid):
        """deletes specified table"""
        self.service.tables().delete(projectId=projectid,
                                     datasetId=datasetid,
                                     tableId=tableid).execute()
