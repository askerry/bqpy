''' pybq.bqdf: defines BQDF class which provides a vaguely pandas-esque
interface with a specific bigquery table

Note: some of this  may become obsolete when https://github.com/pydata/pandas/blob/master/pandas/io/gbq.py
is fully developed and stable (for now gbq only provides basic read/write api)
'''


import pandas as pd
import numpy as np
import sys
sys.path.append('/Users/amyskerry/google-cloud-sdk/platform/bq')
import bq
import matplotlib.pyplot as plt
import seaborn as sns
sns.set_style('white')
from core import run_query, fetch_query, bigquery_connect, max_rows
import util
import bqviz


class BQDF():

    '''Reference to a bigquery table that provides some quick and easy access
    to basic features of the table. Aims to replicate some of the basic functionality
    of pandas dataframe operations and interfacing for return data in df form'''

    def __init__(self, client, tablename, max_rows=max_rows):
        '''initialize a reference to table'''
        self.client = client
        self.tablename = tablename
        self.allstring = "SELECT * FROM %s LIMIT 1" % tablename
        self.max_rows = max_rows
        self.active_col = None
        for col in self.columns:
            funcstr = "lambda self=self:self.set_active_col('%s')" % col
            setattr(BQDF, col, property(eval(funcstr)))

    def query(self, querystr, fetch=True):
        '''execute any arbitary query on the associated table'''
        return bigquery_query(querystr, self.client, fetch=fetch)

    @property
    def columns(self):
        '''returns list of column names from table'''
        with util.Mask_Printing():
            query_response = run_query(self.client, self.allstring)
        fields, data = fetch_query(
            self.client, query_response, start_row=0, max_rows=1)
        return [field['name'] for field in fields]

    @property
    def table_schema(self):
        '''prints datatypes and other settings for each column'''
        with util.Mask_Printing():
            query_response = run_query(self.client, self.allstring)
        fields, data = fetch_query(
            self.client, query_response, start_row=0, max_rows=1)
        print "Table Schema for %s" % self.tablename
        for f in fields:
            others = [
                "%s-%s" % (key, val) for key, val in f.items() if key not in ['type', 'name']]
            print "%s (%s) :   %s" % (f['name'], f['type'], ', '.join(others))
        return fields

    @property
    def size(self):
        '''returns size of the table (# rows, # columns)'''
        with util.Mask_Printing():
            output, source = self.query(
                "SELECT COUNT(*) from %s" % (self.tablename), fetch=False)
        return (int(output.values[0][0]), len(self.columns))

    def groupby(self, groupingcol, operations, max_rows=max_rows):
        '''groups data by grouping column and performs requested operations on other columns
        INPUTS:
            groupingcol (str): column to group on
            operations (list): list of tuples where tuple[0] are columns and tuple[1] are strings representing operations on those columns
        OUTPUTS:
            pandas dataframe: dataframe containing the query results
        '''
        opmap = {'mean': 'AVG', 'std': 'STDDEV', 'sum': 'SUM',
                 'min': 'MIN', 'max': 'MAX', 'count': 'COUNT'}
        operationpairs = [
            "%s(%s) %s_%s " % (opmap[val], key, key, val) for (key, val) in operations]
        grouping_query = "SELECT %s, COUNT(*) count, %s FROM %s GROUP BY %s LIMIT %s" % (
            groupingcol, ', '.join(operationpairs), self.tablename, groupingcol, self.max_rows)
        print grouping_query
        with util.Mask_Printing():
            query_response = run_query(self.client, grouping_query)
        fields, data = fetch_query(
            self.client, query_response, start_row=0, max_rows=self.max_rows)
        return bqresult_2_df(fields, data)

    def join(self, df2, on=None, left_on=None, right_on=None, how='LEFT', fetch=True):
        '''joins table with table referenced in df2 and optionally returns result'''
        if left_on is None:
            left_on = on
            right_on = on
        joinstr = "SELECT * FROM %s df1 %s JOIN %s df2 ON df1.%s=df2.%s" % (
            self.tablename, how, df2.tablename, left_on, right_on)
        print joinstr
        with util.Mask_Printing():
            output, source = self.query(joinstr, fetch=fetch)
        return output, source

    def set_active_col(self, col):
        self.active_col = col
        return self
    
    def clear_active_col(self):
        self.active_col=None

    def count(self, col=None):
        '''return count of non-null entries in column'''
        if col is None:
            col=self.active_col
        with util.Mask_Printing():
            output, source = self.query(
                'SELECT COUNT(%s) from %s' % (col, self.tablename), fetch=False)
        self.clear_active_col()
        return output.values[0][0]

    def min(self, col=None):
        '''return minimum value of column'''
        if col is None:
            col=self.active_col
        with util.Mask_Printing():
            output, source = self.query(
                'SELECT MIN(%s) from %s' % (col, self.tablename), fetch=False)
        self.clear_active_col()
        return output.values[0][0]

    def max(self, col=None):
        '''return maximum value of column'''
        if col is None:
            col=self.active_col
        with util.Mask_Printing():
            output, source = self.query(
                'SELECT MAX(%s) from %s' % (col, self.tablename), fetch=False)
        self.clear_active_col()
        return output.values[0][0]

    def mean(self, col=None):
        '''return mean of column'''
        if col is None:
            col=self.active_col
        with util.Mask_Printing():
            output, source = self.query(
                'SELECT AVG(%s) from %s' % (col, self.tablename), fetch=False)
        self.clear_active_col()
        return output.values[0][0]

    def sum(self, col=None):
        '''return sum of column'''
        if col is None:
            col=self.active_col
        with util.Mask_Printing():
            output, source = self.query(
                'SELECT SUM(%s) from %s' % (col, self.tablename), fetch=False)
        self.clear_active_col()
        return output.values[0][0]

    def std(self, col=None):
        '''return standard deviation of column'''
        if col is None:
            col=self.active_col
        with util.Mask_Printing():
            output, source = self.query(
                'SELECT STDDEV(%s) from %s' % (col, self.tablename), fetch=False)
        self.clear_active_col()
        return output.values[0][0]

    def mode(self, col=None):
        '''return mode of column (if multiple, returns first listed)'''
        if col is None:
            col=self.active_col
        with util.Mask_Printing():
            output, source = self.query('SELECT COUNT(%s) as frequency from %s GROUP BY %s ORDER BY frequency DESC' % (
                col, self.tablename, col), fetch=False)
        self.clear_active_col()
        return output.iloc[0, 0]

    def percentiles(self, col=None):
        '''returns 25th, 50th, and 75t percentiles of column'''
        if col is None:
            col=self.active_col
        with util.Mask_Printing():
            output, source = self.query(
                'SELECT QUANTILEs(%s, 5) from %s' % (col, self.tablename), fetch=False)
        perc_25 = output.iloc[1, 0]
        perc_50 = output.iloc[2, 0]
        perc_75 = output.iloc[3, 0]
        self.clear_active_col()
        return perc_25, perc_50, perc_75

    def describe(self):
        '''replicates df.describe() by returning a dataframe with summary measures for each numeric column'''
        with util.Mask_Printing():
            fields = self.table_schema
        describe_data = {}
        rows = ['count', 'min', '25th percentile', '50th percentile',
                '75th percentile', 'max', 'mean', 'std', 'mode']
        for f in fields:
            if 'INT' in f['type'] or 'LONG' in f['type'] or 'FLOAT' in f['type']:
                column = []
                for func in [self.count, self.min, self.percentiles, self.max, self.mean, self.std, self.mode]:
                    result = func(f['name'])
                    try:
                        column.extend(result)
                    except:
                        column.append(result)
                describe_data[f['name']] = column
        return pd.DataFrame(data=describe_data, index=rows)

    def unique(self, col=None):
        '''find unique values in the requested column'''
        if col is None:
            col=self.active_col
        unique_query = "SELECT %s FROM %s GROUP BY %s" % (
            col, self.tablename, col)
        print unique_query
        with util.Mask_Printing():
            query_response = run_query(self.client, unique_query)
        fields, data = fetch_query(
            self.client, query_response, start_row=0, max_rows=self.max_rows)
        self.clear_active_col()
        return bqresult_2_df(fields, data)[col].values

    def plot(self, grouping_col, value_col, kind='bar'):
        '''plots the mean of value_col (Y), broken down by grouping_col (X) and returns plot axis'''
        plotdf = self.groupby(
            grouping_col, [(value_col, 'mean'), (value_col, 'std'), (value_col, 'count')])
        return bqviz._plot_grouped_data(plotdf, value_col, grouping_col, kind=kind)

    def hist(self, col=None, bins=20, ax=None):
        '''plots a histogram of the desired column, returns the df used for plotting'''
        if col is None:
            col=self.active_col
        binbreaks = self._get_binbreaks(col, bins=bins)
        countstr = _create_sum_str(col, binbreaks)
        querystr = 'SELECT %s FROM %s' % (countstr, self.tablename)
        df, source = self.query(querystr)
        freqs = df.T.iloc[:, 0].values
        labels = ['<' + val[1:] for val in df.T.index.values]
        if ax is None:
            f, ax = plt.subplots(figsize=[8, 4])
        ax.bar(range(len(freqs)), freqs)
        ax.set_xticklabels(labels, rotation=90)
        ax.set_ylabel('frequency')
        ax.set_title("%s histogram" % col)
        self.clear_active_col()
        return df.T

    def _get_binbreaks(self, col, bins=20):
        '''computes breakpoints for binning of data in column'''
        maxval = self.max(col)
        minval = self.min(col)
        interval = float(maxval - minval) / bins
        binbreaks = [minval]
        val = minval
        for i in range(bins):
            val = val + interval
            binbreaks.append(val)
        return binbreaks


def bigquery_query(querystr, client, max_rows=max_rows, fetch=True):
    '''executes a query and returns the results or a result sample as a pandas df

    INPUTS:
        querystr (str):
        client (bq.Client): connection client
        max_rows (int): max number of rows that the client will return in the results
        fetch (bool): if True, fetch the full resultset locally, otherwise return only a sample of the first 10 rows
    OUTPUTS:
        result (pandas datafram): dataframe containing the query results or
            first 10 rows or resultset (if fetch==False)
        destinationtable (dict): remote table that contains the query results
    '''
    query_response = run_query(client, querystr)
    if fetch:
        fields, data = fetch_query(
            client, query_response, start_row=0, max_rows=max_rows)
        return bqresult_2_df(fields, data), query_response['configuration']['query']['destinationTable']

    else:
        fields, data = fetch_query(
            client, query_response, start_row=0, max_rows=10)
        head_sample = bqresult_2_df(fields, data)
        print "query saved to %s" % util.stringify(query_response['configuration']['query']['destinationTable'])
        print "returning head only"
        return head_sample, query_response['configuration']['query']['destinationTable']


def bqresult_2_df(fields, data):
    '''takes the output of the bigquery call and returns data as a dataframe with appropriate dtypes
    INPUTS:
        fields (list): list of dictionaries representing table schema with names and types for each column
        data (list): list of rows of data from table resulting from query
    OUPUTS:
        df (pandas dataframe): dataframe with fields as columns and data as rows
    '''
    format_table = {'STRING': np.str, 'FLOAT': np.float64,
                    'INTEGER': np.int64, 'DATE': 'datetime64[ns]', 'TIMESTAMP': 'datetime64[ns]'}
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


def _create_sum_str(col, binbreaks):
    '''creates string for conditional counting based on binbreaks'''
    sumstrs = []
    for qn, q in enumerate(binbreaks[:-1]):
        minq = q
        maxq = binbreaks[qn + 1]
        sumstrs.append(_create_case_str(col, minq, maxq, qn))
    return ', '.join(sumstrs)


def _create_case_str(col, minq, maxq, qn):
    return 'SUM(CASE WHEN %s>=%.5f and %s<%.5f THEN 1 ELSE 0 END) as %s' % (col, minq, col, maxq, "_%.0f" % (maxq))
