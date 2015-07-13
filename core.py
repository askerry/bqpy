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
    if destination_table is not None:
        print "writing to temporary table %s" % destination_table
    return client.Query(querystr, destination_table=destination_table, dry_run=dry_run)


def fetch_query(client, query_response, start_row=0, max_rows=max_rows):
    fetch_table = query_response['configuration']['query']['destinationTable']
    fields, data = client.ReadSchemaAndRows(
        fetch_table, start_row=start_row, max_rows=max_rows)
    return fields, data


def bigquery_connect(project_id='durable-footing-95814'):
    '''connects to a bigquery client for the provided project_id
    Args:
        project_id (str): project_id from google app developer console
    Returns:
        client (bq.Client): connected client for querying databsae
    '''

    client = bq.Client.Get()
    client.project_id = project_id
    return client
