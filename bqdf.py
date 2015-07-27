''' pybq.bqdf: defines BQDF class which provides a vaguely pandas-esque
interface with a specific bigquery table

Note: some of this  may become obsolete when https://github.com/pydata/pandas/blob/master/pandas/io/gbq.py
is fully developed and stable (for now gbq only provides basic read/write api)
'''


import pandas as pd
import numpy as np
import sys
import util
import bqviz
import cfg
sys.path.append(cfg.gsdk_path)
import bq
import matplotlib.pyplot as plt
import seaborn as sns
sns.set_style('white')
from core import run_query, fetch_query, Connection


class BQDF():

    '''Reference to a bigquery table that provides some quick and easy access
    to basic features of the table. Aims to replicate some of the basic functionality
    of pandas dataframe operations and interfacing for return data in df form'''

    def __init__(self, con, tablename, max_rows=cfg.MAX_ROWS):
        '''initialize a reference to table'''
        self.con = con
        self.tablename = '[%s]' % tablename
        self.remote = tablename
        self.resource = self.get_resource(self.remote)
        self.allstring = "SELECT * FROM [%s] LIMIT 1" % tablename
        self.local = None
        self.max_rows = max_rows
        self.active_col = None
        self.local = self._head()

    def refresh(self):
        self.con.flush_cache()
        self.resource = self.get_resource(self.remote)

    def get_resource(self, remote):
        return self._con.service.tables().get(**util.dictify(remote)).execute()

    @property
    def last_modified(self):
        self.resource = self.get_resource(self.remote)
        print util.convert_timestamp(self.resource['lastModifiedTime'])
        return float(self.resource['lastModifiedTime'])

    @property
    def creation_time(self):
        print util.convert_timestamp(self.resource['creationTime'])

    @property
    def expiration_time(self):
        try:
            print util.convert_timestamp(self.resource['expirationTime'])
        except:
            print "no expiration set"

    def __len__(self):
        return self.resource['numRows']

    def __getitem__(self, index):
        if isinstance(index, list):
            return self._limit_columns(index)
        else:
            self.set_active_col(index)
            return self

    def query(self, querystr, fetch=cfg.FETCH_BY_DEFAULT, dest=None):
        '''execute any arbitary query on the associated table'''
        with util.Mask_Printing():
            output, source, exceeds_max = raw_query(
                self.con, querystr, self.last_modified, dest=dest, fetch=fetch)
            new_bqdf = BQDF(self.con, '%s' % util.stringify(source))
            new_bqdf.local = output
        if exceeds_max:
            print "WARNING: number of rows in remote table exceeds bqdf object's max_rows. Only max_rows have been fetched locally"
        return new_bqdf

    @property
    def values(self, col=None):
        if col is None:
            col = self.active_col
        with util.Mask_Printing():
            output, source, exceeds_max = raw_query(
                self.con, "SELECT %s FROM %s" % (col, self.tablename), self.last_modified)
        return output[col].values

    @property
    def columns(self):
        '''returns list of column names from table'''
        return [f['name'] for f in self.resource['schema']['fields']]

    @property
    def table_schema(self):
        '''prints datatypes and other settings for each column'''
        fields = self.resource['schema']['fields']
        for f in fields:
            others = [
                "%s-%s" % (key, val) for key, val in f.items() if key not in ['type', 'name']]
            print "%s (%s) :   %s" % (f['name'], f['type'], ', '.join(others))
        return fields

    def footprint(self):
        return float(self.resource['numBytes']) / 1048576

    @property
    def size(self):
        '''returns size of the table (# rows, # columns)'''
        return (self.resource['numRows'], len(self.resource['schema']['fields']))

    def _head(self):
        with util.Mask_Printing():
            output, source, _ = raw_query(
                self.con, "SELECT * FROM %s LIMIT 5" % (self.tablename), self.last_modified)
        return output

    def head(self):
        return self.local

    def where(self, *args, **kwargs):
        '''returns data filtered by where statements
        INPUTS:
           args (str): str specifying WHERE clause (e.g. score > 95, name == 'Amy')
           kwargs: fetch (bool): specifies whether to fetch the data locally, dest (dict): specifies destination for results table
        OUTPUTS:
           ndf: BQDF instance for result
        '''
        if 'fetch' in kwargs:
            fetch = kwargs['fetch']
        else:
            fetch = True
        if 'dest' in kwargs:
            dest = kwargs['dest']
        else:
            dest = None
        filter_query = "SELECT * FROM %s WHERE %s" % (
            self.tablename, _create_where_statement(args))
        ndf = self.query(filter_query, fetch=fetch, dest=dest)
        return ndf

    def groupby(self, groupingcol, operations, max_rows=cfg.MAX_ROWS, fetch=True, dest=None):
        '''groups data by grouping column and performs requested operations on other columns
        INPUTS:
            groupingcol (str): column to group on
            operations (list): list of tuples where tuple[0] are columns and tuple[1] are strings representing operations on those columns
        OUTPUTS:
           ndf: BQDF instance for result
        '''
        opmap = {'mean': 'AVG', 'std': 'STDDEV', 'sum': 'SUM',
                 'min': 'MIN', 'max': 'MAX', 'count': 'COUNT'}
        operationpairs = [
            "%s(%s) %s_%s " % (opmap[val], key, key, val) for (key, val) in operations]
        grouping_query = "SELECT %s, %s FROM %s GROUP BY %s LIMIT %s" % (
            groupingcol, ', '.join(operationpairs), self.tablename, groupingcol, self.max_rows)
        ndf = self.query(grouping_query, fetch=fetch, dest=dest)
        return ndf

    def join(self, df2, on=None, left_on=None, right_on=None, how='LEFT', fetch=True, dest=None):
        '''joins table with table referenced in df2 and optionally returns result'''
        if left_on is None:
            left_on = on
            right_on = on
        join_query = "SELECT * FROM %s df1 %s JOIN %s df2 ON df1.%s=df2.%s" % (
            self.tablename, how, df2.tablename, left_on, right_on)
        with util.Mask_Printing():
            ndf = self.query(join_query, fetch=fetch, dest=dest)
        return ndf

    def set_active_col(self, col):
        self.active_col = col
        return self

    def clear_active_col(self):
        self.active_col = None

    def _limit_columns(self, columns, fetch=True, dest=None):
        limit_query = "SELECT %s  FROM %s" % (
            ', '.join(columns), self.tablename)
        ndf = self.query(limit_query, fetch=fetch, dest=dest)
        return ndf

    def topk(self, col, k, fetch=True, dest=None):
        top_query = "SELECT TOP(%s, %s), COUNT(*) as count FROM %s" % (col,
                                                                       k, self.tablename)
        ndf = self.query(top_query, fetch=fetch)
        return ndf

    def _simple_agg(self, col=None, operator='COUNT'):
        if col is None:
            col = self.active_col
        ndf = self.query('SELECT %s(%s) from %s' %
                         (operator, col, self.tablename), fetch=False)
        self.clear_active_col()
        return ndf.local.values[0][0]

    def count(self, col=None):
        '''return count of non-null entries in column'''
        return self._simple_agg(col=col, operator='COUNT')

    def min(self, col=None):
        '''return minimum value of column'''
        return self._simple_agg(col=col, operator='MIN')

    def max(self, col=None):
        '''return maximum value of column'''
        return self._simple_agg(col=col, operator='MAX')

    def mean(self, col=None):
        '''return mean of column'''
        return self._simple_agg(col=col, operator='AVG')

    def sum(self, col=None):
        '''return sum of column'''
        return self._simple_agg(col=col, operator='SUM')

    def std(self, col=None):
        '''return standard deviation of column'''
        return self._simple_agg(col=col, operator='STDDEV')

    def mode(self, col=None):
        '''return mode of column (if multiple, returns first listed)'''
        if col is None:
            col = self.active_col
        ndf = self.query('SELECT COUNT(%s) as frequency from %s GROUP BY %s ORDER BY frequency DESC' % (
            col, self.tablename, col), fetch=False)
        self.clear_active_col()
        return ndf.local.iloc[0, 0]

    def percentiles(self, col=None):
        '''returns 25th, 50th, and 75t percentiles of column'''
        if col is None:
            col = self.active_col
        ndf = self.query(
            'SELECT QUANTILEs(%s, 5) from %s' % (col, self.tablename), fetch=False)
        perc_25 = ndf.local.iloc[1, 0]
        perc_50 = ndf.local.iloc[2, 0]
        perc_75 = ndf.local.iloc[3, 0]
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
            col = self.active_col
        unique_query = "SELECT %s FROM %s GROUP BY %s" % (
            col, self.tablename, col)
        ndf = self.query(unique_query)
        self.clear_active_col()
        return ndf.local[col].values

    def plot(self, grouping_col, value_col, kind='bar'):
        '''plots the mean of value_col (Y), broken down by grouping_col (X) and returns plot axis'''
        plotdf = self.groupby(
            grouping_col, [(value_col, 'mean'), (value_col, 'std'), (value_col, 'count')])
        return bqviz._plot_grouped_data(plotdf.local, value_col, grouping_col, kind=kind)

    def hist(self, col=None, bins=20, ax=None):
        '''plots a histogram of the desired column, returns the df used for plotting'''
        if col is None:
            col = self.active_col
        binbreaks = self._get_binbreaks(col, bins=bins)
        countstr = _create_full_str(col, binbreaks, kind='count')
        querystr = 'SELECT %s FROM %s' % (countstr, self.tablename)
        ndf = self.query(querystr)
        bqviz.plot_hist(ndf.local, col, ax=ax)
        self.clear_active_col()
        return ndf.local.T

    def scatter(self, x=None, y=None, bins=200, ax=None):
        '''plots a scatter plot of x vs y (downsampled if data.size>bins, returns the series used for plotting'''
        if self.__len__() > bins:
            binbreaks = self._get_binbreaks(x, bins=bins)
            meanstr = _create_full_str(x, binbreaks, kind='mean', ycol=y)
            stdstr = _create_full_str(x, binbreaks, kind='std', ycol=y)
            countstr = _create_full_str(x, binbreaks, kind='count', ycol=y)
            querystr = 'SELECT %s FROM %s' % (meanstr, self.tablename)
            scatterdf = self.query(querystr)
            errorstr = 'SELECT %s FROM %s' % (stdstr, self.tablename)
            error = self.query(errorstr).local.T[0]
            countstr = 'SELECT %s FROM %s' % (countstr, self.tablename)
            counts = self.query(countstr).local.T[0]
            sems = [s / np.sqrt(c) for s, c in zip(error, counts)]
            plotdf = bqviz.plot_scatter(
                scatterdf.local, x, y, ax=ax, downsampled=True, error=sems, counts=counts)
        else:
            querystr = 'SELECT %s, %s FROM %s' % (x, y, self.tablename)
            scatterdf = self.query(querystr)
            plotdf = bqviz.plot_scatter(
                scatterdf.local, x, y, ax=ax, downsampled=False)
        self.clear_active_col()
        return plotdf

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


def bqresult_2_df(fields, data):
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


def _create_full_str(col, binbreaks, kind='count', ycol=None):
    '''creates string for conditional counting based on binbreaks'''
    fullstrs = []
    for qn, q in enumerate(binbreaks[:-1]):
        minq = q
        maxq = binbreaks[qn + 1]
        if kind == 'count':
            fullstrs.append(_create_case_str_count(col, minq, maxq, qn))
        elif kind == 'mean':
            fullstrs.append(_create_case_str_mean(col, ycol, minq, maxq, qn))
        elif kind == 'std':
            fullstrs.append(_create_case_str_std(col, ycol, minq, maxq, qn))

    return ', '.join(fullstrs)


def _create_case_str_count(col, minq, maxq, qn):
    return 'SUM(CASE WHEN %s>=%.5f and %s<%.5f THEN 1 ELSE 0 END) as %s' % (col, minq, col, maxq, "_%.0f" % (maxq))


def _create_case_str_mean(xcol, ycol, minq, maxq, qn):
    return 'AVG(CASE WHEN %s>=%.5f and %s<%.5f THEN %s ELSE NULL END) as %s' % (xcol, minq, xcol, maxq, ycol, "_%.0f" % ((minq + maxq) / 2))


def _create_case_str_std(xcol, ycol, minq, maxq, qn):
    return 'STDDEV(CASE WHEN %s>=%.5f and %s<%.5f THEN %s ELSE NULL END) as %s' % (xcol, minq, xcol, maxq, ycol, "_%.0f" % ((minq + maxq) / 2))


def _create_where_statement(*args):
    operations = ['==', '>', '<', '>=', '<=', '!=']
    wheres = []
    for expression in args[0]:
        for o in operations:
            try:
                output = expression.split(o)
                operation = o
                col = output[0].strip()
                try:
                    val = float(output[1].strip())
                except:
                    val = '"%s"' % output[1].strip()
                wheres.append(_create_single_where(col, val, operation))
                break
            except:
                pass
    return ' AND '.join(wheres)


def _create_single_where(key, value, operation):
    return '%s %s %s' % (key, operation, value)


def raw_query(con, querystr, last_modified, dest=None, max_rows=cfg.MAX_ROWS, fetch=cfg.FETCH_BY_DEFAULT):
    '''executes a query and returns the results or a result sample as a pandas df and the destination table as a dict

    INPUTS:
        querystr (str):
        dest (dict): specify destination table for output of query (if None, BQ creates a temporary (24hr) table)
        max_rows (int): max number of rows that the con will return in the results
        fetch (bool): if True, fetch the full resultset locally, otherwise return only a sample of the first 10 rows
    OUTPUTS:
        result (pandas datafram): dataframe containing the query results or
            first 10 rows or resultset (if fetch==False)
        destinationtable (dict): remote table that contains the query results
    '''
    exists = con._check_query(querystr, fetch, last_modified)
    if not exists:
        query_response = run_query(con, querystr, destination_table=dest)
        if fetch:
            fields, data = fetch_query(
                con, query_response, start_row=0, max_rows=max_rows)
            df, source = bqresult_2_df(fields, data), query_response[
                'configuration']['query']['destinationTable']
            con._cache_query(querystr, df, source, fetch)
            con._log_query(querystr)
            if con._service.tables().get(source).execute()['numRows'] > max_rows:
                exceeds_max_rows = True
            else:
                exceeds_max_rows = False
            return df, source, exceeds_max_rows

        else:
            fields, data = fetch_query(
                con, query_response, start_row=0, max_rows=10)
            head_sample = bqresult_2_df(fields, data)
            print "Query saved to %s." % util.stringify(query_response['configuration']['query']['destinationTable'])
            print "Returning head only."
            df, source = head_sample, query_response[
                'configuration']['query']['destinationTable']
            con._cache_query(querystr, df, source, fetch)
            con._log_query(querystr)
            exceeds_max_rows = True
            return df, source, exceeds_max_rows
    else:
        return con._fetch_from_cache(querystr)
