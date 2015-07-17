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


def run_query(client, querystr, destination_table=None, dry_run=False):
    '''executes a query on remote bq dataset

    INPUTS:
        client (bq.Client): connected client for querying database
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
    query_response = client.Query(
        querystr, destination_table=destination_table, dry_run=dry_run)
    util._log_query(client, query_response)
    return query_response


def fetch_query(client, query_response, start_row=0, max_rows=cfg.MAX_ROWS):
    '''fetches the results specified in the query response and returns them locally

    INPUTS:
        client (bq.Client): connected client for querying database
        query_response (dict): dictionary specifying query_response (output of run_query), 
            including remote table containing the query results
        start_row (int): row to start at when returning results
        max_rows (int): max # of rows to fetch
    OUPUTS:
        fields (list): list of dictionaries representing table schema with names and types for each column
        data (list): list of rows of data from table resulting from query
    '''
    fetch_table = query_response['configuration']['query']['destinationTable']
    fields, data = client.ReadSchemaAndRows(
        fetch_table, start_row=start_row, max_rows=max_rows)
    return fields, data


def bigquery_connect(project_id=None, logging_file=None, cache_max=None):
    '''connects to a bigquery client for the provided project_id

    INPUTS:
        project_id (str): project_id from google app developer console
    OUPUTS:
        client (bq.Client): connected client for querying database
    '''

    client = bq.Client.Get()
    client.project_id = project_id
    client.logging_file = logging_file
    client.querycache = {}
    client.querylog = {}
    if cache_max is None:
        client.cache_max = cfg.CACHE_MAX
    else:
        client.cache_max = cache_max

    def update(self):
        self.flush_cache()

    def _cache_query(self, querystr, df, source, fetch):
        if self.cache_max > 0:
            i = 0
            while float(asizeof(self.querycache)) / 1048576 >= self.cache_max:
                del self.querycache[self.querycache.keys()[i]]
                i += 1
                self.querycache[querystr] = {}
                self.querycache[querystr]['local'] = df
                self.querycache[querystr]['source'] = source
                self.querycache[querystr]['fetched'] = fetch
                self.service = util.get_service()

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

    for func in [update, _cache_query, _log_query, _flush_cache, _check_query, _fetch_from_cache, util.list_projects, util.list_datasets, util.list_tables, util.delete_table]:
        client = util._monkey_patch_to_instance(func, client)
    return client
