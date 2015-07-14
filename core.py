''' pybq.core: core functions for querying and fetching data from bigquery
'''


import pandas as pd
import numpy as np
import sys
sys.path.append('/Users/amyskerry/google-cloud-sdk/platform/bq')
import bq
import matplotlib.pyplot as plt
import seaborn as sns
sns.set_style('white')

max_rows = 10000000  # max rows to ever return locally


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
    return client.Query(querystr, destination_table=destination_table, dry_run=dry_run)


def fetch_query(client, query_response, start_row=0, max_rows=max_rows):
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


def bigquery_connect(project_id='durable-footing-95814'):
    '''connects to a bigquery client for the provided project_id

    INPUTS:
        project_id (str): project_id from google app developer console
    OUPUTS:
        client (bq.Client): connected client for querying database
    '''

    client = bq.Client.Get()
    client.project_id = project_id
    return client
