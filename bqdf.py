''' pybq.bqdf: defines BQDF class which provides a vaguely pandas-esque
interface with a specific bigquery table

Note: some of this  may become obsolete when https://github.com/pydata/pandas/blob/master/pandas/io/gbq.py
is fully developed and stable (for now gbq only provides basic read/write api)
'''


import pandas as pd
import numpy as np
import sys
import itertools
import util
import bqviz
from time import time, sleep
import cfg
import warnings
import copy
sys.path.append(cfg.gsdk_path)
import bq
import matplotlib.pyplot as plt
import seaborn as sns
sns.set_style('white')
from core import run_query, fetch_query, Connection, create_column_from_values, bqresult_2_df, write_df_to_remote, get_table_resource


# TODO: think about (and communicate to user about) efficiency/pricing
# tradeoffs this approach makes
# - NOTE:  in many cases, what could be a larger(and possibly more efficient) single query is broken down into intermediate steps. But this can in principle have an efficiency advantage since the intermediate computations(e.g. a mean of some column) can be cached and reused for different higher level operations. Need to understand pricing specifics more to optimize this.

##########################################################################
# ####################### BigQuery DataFrame Class #######################
##########################################################################


class BQDF(object):

    '''Reference to a bigquery table that provides some quick and easy access
    to basic features of the table. Aims to replicate some of the basic functionality
    of pandas dataframe operations and interfacing for return data in df form'''

    def __init__(self, con, tablename, max_rows=cfg.MAX_ROWS, fill=True):
        '''initialize a reference to table'''
        self.con = con
        self.tablename = '[%s]' % tablename
        self.remote = tablename
        self.resource = None
        self.allstring = "SELECT * FROM [%s] LIMIT 1" % tablename
        self.local = None
        self.max_rows = max_rows
        self.fetched = False
        self.active_col = None
        self.local = None
        self.hidden = []
        if fill:
            self.resource = self.get_resource(self.remote)
            self.local = self._head()

########################################
# ######    MAIN TABLE ACCESS    #######
########################################

    def __getitem__(self, index):
        if isinstance(index, list):
            return self.query('SELECT %s FROM %s' % (', '.join(index), self.tablename), fetch=self.fetched)
        elif isinstance(index, tuple):
            raise ValueError(
                "Trying to access with a tuple. Did you mean [%s]" % list(index))
        elif index in self.columns:
            self._set_active_col(index)
            return self
        elif isinstance(index, int):
            return self._get_nth_row(index)

    def __delitem__(self, colname):
        '''Delete column from table'''
        if colname not in self.columns:
            raise NameError("%s is not a column in table" % colname)
        newcols = [col for col in self.columns if col != colname]
        newdf = self.query('SELECT %s from %s' % (', '.join(
            newcols), self.remote), fetch=False, dest=self.remote, overwrite_method='overwrite')
        self.refresh()

    def query(self, querystr, fetch=cfg.FETCH_BY_DEFAULT, dest=None, fill=True, overwrite_method='fail'):
        '''execute any arbitary query on the associated table'''
        self.fetched = fetch
        with util.Mask_Printing():
            output, source, exceeds_max = raw_query(
                self.con, querystr, self.last_modified, dest=dest, fetch=fetch, overwrite_method=overwrite_method)
            new_bqdf = BQDF(self.con, '%s' % util.stringify(source), fill=fill)
            new_bqdf.local = output
            new_bqdf.fetched = fetch
        if exceeds_max:
            pass  # TODO figure how why exceeds_max isn't behaving as expected
            # print "Number of rows in remote table exceeds bqdf object's
            # max_rows. Only max_rows have been fetched locally"
        return new_bqdf

    def rename(self, rename_columns, inplace=False):
        '''rename columns'''
        # NOTE: involves full table scan
        if inplace:
            dest = self.remote
        else:
            dest = None
        cols = [col for col in self.columns if col not in rename_columns]
        renames = ["%s as %s" % (key, columns[key]) for key in rename_columns]
        querystr = "SELECT %s FROM %s" (
            ', '.join(renames + cols), self.tablename)
        newdf = self.query(
            querystr, fetch=self.fetched, dest=dest, overwrite_method='overwrite')
        if not inplace:
            return newdf
        else:
            self.refresh()

    def add_col(self, column=None, content=1, replace=False, inplace=True):
        '''add new column to the table '''
        # TODO think about alternative implementation (major constraint: can't change existing rows of a table)
        # TODO warn/prompt for permission to perform these more costly
        # operations?
        if column in self.columns:
            if replace:
                del self[column]
            else:
                raise NameError("%s is already a column in table" % column)
        newremote, length = self._get_remote_reference(content, column)
        # with util.Mask_Printing():
        df = self._add_col_join(newremote, column, inplace)
        if not inplace:
            return df
        else:
            self.refresh()

    def replace(self, column=None, content=1):
        '''adds a column that replaces an existing column'''
        self.add_col(column, content, replace=True)

    def slice(self, start=0, end=10):
        # NOTE need to fit slice locally
        # see if there is a bigquery way to do this
        fields, data = self.con.client.ReadSchemaAndRows(
            util.dictify(self.remote), start_row=start, max_rows=end - start)
        ndf = bqresult_2_df(fields, data)
        dest = self.remote + '_slice_%sto%s' % (start, end)
        _ = write_df_to_remote(self.con, ndf, **util.dictify(dest))
        if not self._check_write(dest):
            warnings.warn('failed to write new slice to bigquery')
        ndf = BQDF(self.con, dest)
        ndf.refresh()
        return ndf

    def save(self, project_id, dataset_id, table_id):
        '''copy table (useful for saving a bqdf currently pointing to a temporary table'''
        pass


########################################
# ######   FILTER, GROUP, APPLY  #######
########################################

    def where(self, *args, **kwargs):
        '''returns data filtered by where statements
        INPUTS:
           args (str): str specifying WHERE clause (e.g. score > 95, name == 'Amy')
           kwargs: fetch (bool): specifies whether to fetch the data locally, dest (dict): specifies destination for results table
        OUTPUTS:
           ndf: BQDF instance for result
        '''
        fetch = kwargs.get('fetch', True)
        dest = kwargs.get('dest', None)
        columns = kwargs.get('columns', self.columns)
        filter_query = "SELECT %s FROM %s WHERE %s" % (', '.join(columns),
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
        operationpairs = []
        for (key, val) in operations:
            if val == 'sem':
                operationpairs.append(
                    "STDDEV(%s)/SQRT(COUNT(%s) %s_sem " % (key, key, key))  # TODO figure out nan handling
            else:
                operationpairs.append(
                    "%s(%s) %s_%s " % (opmap[val], key, key, val))
        grouping_query = "SELECT %s, %s FROM %s GROUP BY %s" % (
            groupingcol, ', '.join(operationpairs), self.tablename, groupingcol)
        ndf = self.query(grouping_query, fetch=fetch, dest=dest)
        return ndf

    def apply(self, func, col=None, columns=None, max_rows=cfg.MAX_ROWS, fetch=True, dest=None, chunksize=10000):
        '''idea is to (in a majorly hacky way) allow arbitrary python "udfs" but pulling each row locally and applying the python function, then writing back to bq'''
        # TODO make work and allow user to provide arguments to function
        if col is None:
            col = self.active_col
        startrow = 0
        while startrow < len(self):
            fields, data = self.con.client.ReadSchemaAndRows(
                util.dictify(self.remote), start_row=startrow, max_rows=chunksize)
            ndf = bqresult_2_df(fields, data)
            ndf[col + '_mod'] = ndf[col].apply(func)
            if dest is None:
                dest = self.remote + '_mod_%s' % col
            ndf = ndf[[col + '_mod']]
            _, _ = write_df_to_remote(
                self.con, ndf, overwrite_method='append', **util.dictify(dest))
            startrow += chunksize
        if not self._check_write(dest):
            warnings.warn('remote writing of UDF apply function failed')
        combined_df = BQDF(self.con, dest)
        return combined_df

    def groupby_apply(self, groupingcol, func, columns=None, max_rows=cfg.MAX_ROWS, fetch=True, dest=None):
        ''' same as apply (python udf hack) but for groups analogous to df.groupby('col').apply(myfunc)
        #TODO make work and allow user to provide arguments
        groups data by grouping column and performs requested operations on other columns
        INPUTS:
            groupingcol (str): column to group on
            func (python function): takes arbitrary python function that acts on all data in a group
            columns (list): list of column names to touch with function
        OUTPUTS:
           ndf: BQDF instance for result
        '''
        dest = None
        if columns is None:
            columns is self.columns
        for group in self.unique(groupingcol):
            group_query = "SELECT %s FROM %s WHERE  %s == %s" (
                ', '.join(columns), self.tablename, groupingcol, group)
            ndf = self.query(group_query, fetch=True, dest=dest)
            applied_ndf = func(ndf.local)
            if dest is None:
                gdf = self.query(group_query, fetch=True, dest=None)
                dest = gdf.remote
            _, _ = write_df_to_remote(
                self.con, applied_ndf, overwrite_method='append', **util.dictify(dest))
        if not self._check_write(dest):
            warnings.warn(
                'remote writing of UDF groupby-apply function failed')
        gdf = BQDF(self.con, '%s' % dest)
        return gdf

    def join(self, df2, on=None, left_on=None, right_on=None, how='LEFT', dest=None, inplace=True):
        '''joins table with table referenced in df2 and optionally returns result'''
        if inplace:
            dest = self.remote
            overwrite_method = 'overwrite'
        else:
            overwrite_method = 'fail'
        if left_on is None:
            left_on, right_on = on, on
        dups = list(set(self.columns).intersection(set(df2.columns)))
        fulldups = [x for j in [['df1.' + i, 'df2.' + i]
                                for i in dups] for x in j]
        allcols = [
            c for c in self.columns + df2.columns + fulldups if c not in dups]
        join_query = "SELECT %s FROM %s df1 %s JOIN %s df2 ON df1.%s=df2.%s" % (', '.join(allcols),
                                                                                self.tablename, how, df2.tablename, left_on, right_on)
        with util.Mask_Printing():
            ndf = self.query(
                join_query, fetch=self.fetched, dest=dest, overwrite_method=overwrite_method)
        if inplace:
            self.refresh()
        else:
            return ndf

    def add_index(self, colname='index', inplace=False):
        '''add an index (can be used to join and add new columns)'''
        # TODO any way make this not involve full table scan for all columns?
        if inplace:
            dest = self.remote
            overwrite_method = 'overwrite'
        else:
            dest = None
            overwrite_method = 'fail'
        ndf = self.query('SELECT ROW_NUMBER() OVER() %s, * from %s' % (colname,
                                                                       self.tablename), fetch=self.fetched, dest=dest, overwrite_method=overwrite_method)
        if inplace:
            self.refresh()
        else:
            return ndf

    def dropna(self, dest=None):
        filters = ["%s IS NOT NULL" % c for c in self.columns]
        filterstr = ' AND '.join(filters)
        ndf = self.query('SELECT * FROM %s WHERE %s' % (self.tablename, filterstr),
                         fetch=self.fetched, dest=dest, overwrite_method=overwrite_method)
        return ndf

    def split_unstack(self, col=None, delimiter=' '):
        if col is None:
            col = self.active_col
        ndf = self.query('SELECT SPLIT(%s, "%s") as split from %s' %
                         (col, delimiter, self.tablename), fetch=fetch, fill=False)
        return ndf

    def sort_by(self, col, desc=True, columns=None):
        if columns is None:
            columns = self.columns
        if desc:
            querystr = 'SELECT %s from %s ORDER BY %s DESC' % (
                ', '.join(columns), self.tablename, col)
        else:
            querystr = 'SELECT %s from %s ORDER BY %s' % (
                ', '.join(columns), self.tablename, col)
        ndf = self.query(querystr, fetch=fetch, fill=False)
        return ndf

########################################
# ######     PAIR OPERATIONS     #######
########################################

    def corr(self, col1, col2):
        '''compute pearson correlation between two columns'''
        ndf = self.query('SELECT CORR(%s, %s) from %s' %
                         (col1, col2, self.tablename), fetch=False, fill=False)
        return ndf.local.values[0][0]

    def cos(self, col1, col2):
        '''compute cosine similarity between two columns'''
        # cos-theta = dot(col1, col2) / ||col1|| * ||col2||)
        dot = self.dot(col1, col2)
        mag = self.query('SELECT SQRT(SUM(POW(%s,2))) * SQRT(SUM(POW(%s,2))) from %s WHERE %s is not NULL and %s is not NULL' %
                         (col1, col2, self.tablename, col1, col2), fetch=False, fill=False)
        return dot / mag.local.values[0][0]

    def dot(self, col1, col2):
        '''compute dot product between two columns'''
        ndf = self.query('SELECT SUM(%s * %s) from %s WHERE %s is not NULL and %s is not NULL' %
                         (col1, col2, self.tablename, col1, col2), fetch=False, fill=False)
        return ndf.local.values[0][0]

    def euclidean(self, col1, col2):
        '''compute euclidean distance between two columns'''
        ndf = self.query('SELECT SQRT(SUM(POW((%s-%s),2))) from %s WHERE %s is not NULL and %s is not NULL' %
                         (col1, col2, self.tablename, col1, col2), fetch=False, fill=False)
        return ndf.local.values[0][0]

    def contingency_mat(self, col1, col2, dest=None):
        '''creates a contingency matrix for col1 and col2'''
        contingency_query = "SELECT %s, %s, COUNT(*) count FROM %s GROUP BY %s, %s" % (
            col1, col2, self.tablename, col1, col2)
        ndf = self.query(contingency_query, fetch=True, dest=dest).local
        return ndf.pivot(col1, col2, 'count')


########################################
# ######    SERIES OPERATIONS    #######
########################################

    def subtract(self, col1, col2, fetch=cfg.FETCH_BY_DEFAULT):
        ndf = self.query('SELECT %s - %s as diff from %s' %
                         (col1, col2, self.tablename), fetch=fetch, fill=False)
        return ndf

    def add(self, col1, col2, fetch=cfg.FETCH_BY_DEFAULT):
        ndf = self.query('SELECT %s + %s as diff from %s' %
                         (col1, col2, self.tablename), fetch=fetch, fill=False)
        return ndf

    def divide(self, col1, col2, fetch=cfg.FETCH_BY_DEFAULT):
        ndf = self.query('SELECT %s / %s as div from %s' %
                         (col1, col2, self.tablename), fetch=fetch, fill=False)
        return ndf

    def multiply(self, col1, col2, fetch=cfg.FETCH_BY_DEFAULT):
        ndf = self.query('SELECT %s * %s as product from %s' %
                         (col1, col2, self.tablename), fetch=fetch, fill=False)
        return ndf

    def abs(self, col=None, fetch=cfg.FETCH_BY_DEFAULT):
        '''compute absolute value of the column'''
        if col is None:
            col = self.active_col
        ndf = self.query('SELECT ABS(%s) as abs from %s' %
                         (col, self.tablename), fetch=fetch, fill=False)
        self._clear_active_col()
        return ndf

    def sqrt(self, col=None, fetch=cfg.FETCH_BY_DEFAULT):
        '''compute square root of the column'''
        if col is None:
            col = self.active_col
        ndf = self.query('SELECT SQRT(%s) as sqrt from %s' %
                         (col, self.tablename), fetch=fetch, fill=False)
        self._clear_active_col()
        return ndf

    def round(self, col=None, dig=0, fetch=cfg.FETCH_BY_DEFAULT):
        '''round column to specified digit'''
        if col is None:
            col = self.active_col
        ndf = self.query('SELECT ROUND(%s, %s) as round from %s' %
                         (col, dig, self.tablename), fetch=fetch, fill=False)
        self._clear_active_col()
        return ndf

    def pow(self, col=None, power=2, fetch=cfg.FETCH_BY_DEFAULT):
        '''compute column values to specified power'''
        if col is None:
            col = self.active_col
        ndf = self.query('SELECT pow(%s, %s) as pow from %s' %
                         (col, power, self.tablename), fetch=fetch, fill=False)
        self._clear_active_col()
        return ndf

    def log(self, col=None, base='e', fetch=cfg.FETCH_BY_DEFAULT):
        '''compute log of the column values'''
        if col is None:
            col = self.active_col
        logs = {'e': 'ln', 2: 'log2', 10: 'log10'}
        try:
            func = logs[base]
        except KeyError:
            raise NameError("log base %s is not supported" % base)
        ndf = self.query('SELECT %s(%s) as log from %s' %
                         (func, col, self.tablename), fetch=fetch, fill=False)
        self._clear_active_col()
        return ndf

    def zscore(self, col=None):
        '''compute zscore of the column'''
        if col is None:
            col = self.active_col
        avg = self.query('SELECT AVG(%s) from %s' %
                         (col, self.tablename), fetch=False).local.iloc[0, 0]
        std = self.query('SELECT STDDEV(%s) from %s' %
                         (col, self.tablename), fetch=False).local.iloc[0, 0]
        ndf = self.query('SELECT (%s-%s)/%s zscore from %s' %
                         (col, avg, std, self.tablename), fetch=False)
        self._clear_active_col()
        return ndf

    def lower(self, col=None):
        if col is None:
            col = self.active_col
        ndf = self.query('SELECT LOWER(%s) as lower from %s' %
                         (col, self.tablename), fetch=fetch, fill=False)
        return ndf

    def upper(self, col=None):
        if col is None:
            col = self.active_col
        ndf = self.query('SELECT UPPER(%s) as upper from %s' %
                         (col, self.tablename), fetch=fetch, fill=False)
        return ndf

    def replace_str(self, string_to_replace, replacement_string, col=None):
        '''replace string_to_replace with replacement_string'''
        if col is None:
            col = self.active_col
        ndf = self.query('SELECT REPLACE(%s, "%s", "%s") as lower from %s' %
                         (col, string_to_replace, replacement_string, self.tablename), fetch=fetch, fill=False)
        return ndf

    def str_index(self, search_string, col=None):
        '''get 1-based index of first occurence of search_string in the column (0 if not present)'''
        if col is None:
            col = self.active_col
        ndf = self.query('SELECT INSTR(%s, "%s") as lower from %s' %
                         (col, search_string, self.tablename), fetch=fetch, fill=False)
        return ndf


########################################
# ######   AGGREGATE FUNCTIONS   #######
########################################

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

    def sem(self, col=None):
        '''return standard error of the mean of column'''
        if col is None:
            col = self.active_col
        ndf = self.query('SELECT STDDEV(%s)/SQRT(COUNT(%s)) from %s' %
                         (col, col, self.tablename), fetch=False, fill=False)
        self._clear_active_col()
        return ndf.local.values[0][0]

    def mode(self, col=None):
        '''return mode of column (if multiple, returns first listed)'''
        if col is None:
            col = self.active_col
        ndf = self.query('SELECT %s, COUNT(%s) as frequency from %s GROUP BY %s ORDER BY frequency DESC' % (
            col, col, self.tablename, col), fetch=False)
        self._clear_active_col()
        return ndf.local.iloc[0, 0]

    def percentiles(self, col=None):
        '''returns 25th, 50th, and 75t percentiles of column'''
        if col is None:
            col = self.active_col
        ndf = self.query(
            'SELECT QUANTILEs(%s, 5) from %s' % (col, self.tablename), fetch=False)
        try:
            perc_25 = ndf.local.iloc[1, 0]
            perc_50 = ndf.local.iloc[2, 0]
            perc_75 = ndf.local.iloc[3, 0]
        except IndexError:
            perc_25, perc_50, perc_75 = np.nan, np.nan, np.nan
        self._clear_active_col()
        return perc_25, perc_50, perc_75


########################################
# ######       STATISTICS        #######
########################################

# TODO: write tests for all stats and handle nan values correctly

    def ttest_1samp(self, col, nullhypothesis=0):
        #(mean-nullhyp)/(std/sqrt(n))
        pass

    def ttest_ind(self, col1, col2):
        # pooled variance ttest
        #(mean_1 - mean_2)/(std_12 * sqrt(1/n_1 +1/n_2))
        #std_12 = sqrt( ((n1-1)*std_1 + (n2-1)*std_2)/( n_1 + n_2 - 2) )
        pass

    def ttest_rel(self, col1, col2):
        #(meandiff)/(std_diff/sqrt(n))
        pass

    def chi_square(self, col1, col2):
        pass

    def binomial(self, col, p=.5):
        pass

    def pearsonr(self, col1, col2):
        pass

    def onewayanova(self, valuecol, factor):
        pass

    def twowayanova(self, valuecol, factor1, factor2):
        pass

    def rmanova(self, valuecol, withinfactor, betweenfactor):
        pass

    def linear_regression(self, y, xcols):
        pass


########################################
# #####  DATETIME FUNCTIONALITY  #######
########################################

# TODO

# easy time windowing
# dal, wal, etc.

########################################
# #####  BASIC EXPLORATION FUNCS #######
########################################

    def _head(self):
        with util.Mask_Printing():
            output, source, _ = raw_query(
                self.con, "SELECT * FROM %s LIMIT 5" % (self.tablename), self.last_modified)
        return output

    def head(self):
        return self.local.head()

    @property
    def values(self, col=None):
        '''return values from single column'''
        if col is None:
            col = self.active_col
        with util.Mask_Printing():
            output, source, exceeds_max = raw_query(
                self.con, "SELECT %s FROM %s" % (col, self.tablename), self.last_modified, fetch=True)
        return output[col].values

    @property
    def columns(self):
        '''returns list of column names from table'''
        return [f['name'] for f in self.resource['schema']['fields'] if f['name']]

    def table_schema(self):
        '''prints datatypes and other settings for each column'''
        fields = self.resource['schema']['fields']
        for f in fields:
            others = [
                "%s-%s" % (key, val) for key, val in f.items() if key not in ['type', 'name']]
            print "%s (%s) :   %s" % (f['name'], f['type'], ', '.join(others))
        return fields

    def describe(self):
        '''replicates df.describe() by returning a dataframe with summary measures for each numeric column'''
        # TODO this is super inefficient. investigate percentile options.
        with util.Mask_Printing():
            fields = self.table_schema()
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

    def unique(self, col=None, fetch=True):
        '''find unique values in the requested column'''
        if col is None:
            col = self.active_col
        unique_query = "SELECT %s FROM %s GROUP BY %s" % (
            col, self.tablename, col)
        with util.Mask_Printing():
            ndf = self.query(unique_query, fetch=fetch)
        self._clear_active_col()
        return ndf.local[col].values

    def topk(self, k, col=None, fetch=True, dest=None):
        if col is None:
            col = self.active_col
        top_query = "SELECT TOP(%s, %s) %s, COUNT(*) as count FROM %s" % (col,
                                                                          k, col, self.tablename)
        with util.Mask_Printing():
            ndf = self.query(top_query, fetch=True)
        return ndf

    def sample(self, uniquecol, p=.10,  columns=None):
        '''generate a random sample of the data'''
        if columns is None:
            columns = self.columns
        modval = int(round(1 / p))
        querystr = 'SELECT %s FROM %s WHERE HASH(STRING(%s)) \% %s == 0' % (
            ', '.join(columns), self.tablename, uniquecol, modval)
        return self.query(querystr)


########################################
# ######      VISUALIZATION      #######
########################################

    def corr_mat(self, plot=True):
        '''compute correlation matrix between all numeric table columns'''
        numerics = [col for col in self.columns if self.local[
            col].dtype in (np.int64, np.float64)]
        mat = pd.DataFrame(columns=numerics, index=numerics, data=1)
        for col1, col2 in itertools.combinations(numerics, 2):
            r = self.corr(col1, col2)
            mat.loc[col1, col2] = r
            mat.loc[col2, col1] = r
        if plot:
            bqviz.plot_matrix(mat)
        return mat

    def qqplot(self, col=None):
        if col is None:
            col = self.active_col
        # TODO implement

    def gridplot(self):
        return bqviz._gridplot(self)

    def plot(self, grouping_col, value_col, kind='bar', ax=None):
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
        self._clear_active_col()
        return ndf.local.T

    def scatter(self, x=None, y=None, bins=200, ax=None):
        '''plots a scatter plot of x vs y (downsampled if data.size>bins, returns the series used for plotting'''
        if self.__len__() > bins:
            binbreaks = self._get_binbreaks(x, bins=bins)
            meanstr = _create_full_str(x, binbreaks, kind='mean', ycol=y)
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
        self._clear_active_col()
        return plotdf


########################################
# ######    TABLE PROPERTIES     #######
########################################

    @property
    def size(self):
        '''returns size of the table (# rows, # columns)'''
        return (int(self.resource['numRows']), len(self.resource['schema']['fields']))

    @property
    def last_modified(self):
        self.resource = self.get_resource(self.remote)
        print util.convert_timestamp(self.resource['lastModifiedTime'])
        return float(self.resource['lastModifiedTime'])

    @property
    def creation_time(self):
        '''Creation time for the table'''
        print util.convert_timestamp(self.resource['creationTime'])

    @property
    def expiration_time(self):
        '''Expiration time for the table'''
        try:
            print util.convert_timestamp(self.resource['expirationTime'])
        except KeyError:
            warnings.warn("No expiration set")

    def __len__(self):
        '''length of table (# of rows)'''
        try:
            return int(self.resource['numRows'])
        except KeyError:
            with util.Mask_Printing():
                output, source, exceeds_max = raw_query(
                    self.con, 'SELECT COUNT(*) FROM %s' % self.tablename, self.last_modified)
            return output.values[0][0]

    def footprint(self):
        '''check size of table'''
        return float(self.resource['numBytes']) / 1048576


########################################
# ######       UTILITIES         #######
########################################

    def flush(self):
        '''flush cache (will not affect bigquery cache, only affects local caching to prevent excessive network burden)'''
        self.con.flush_cache()
        self.resource = self.get_resource(self.remote)

    def get_resource(self, remote):
        '''fetch info about remote table'''
        return self.con.client._apiclient.tables().get(**util.dictify(remote)).execute()

    def refresh(self):
        '''refresh the local state of the table'''
        if self.fetched:
            self.fetch()
        else:
            self.local = self._head()

    def fetch(self):
        '''overwrite table with columns specified in bqdf.columns'''
        ndf = self.query('select %s from %s' % (', '.join(
            self.columns), self.tablename), fetch=True, dest=self.remote, overwrite_method='overwrite')
        self.fetched = True
        self.local = ndf.local


########################################
# ######     INTERNAL METHODS    #######
########################################

    def _get_remote_reference(self, content, column):
        '''get reference to a remote table containing new column content'''
        if isinstance(content, BQDF):
            if len(content.columns) > 1:
                raise ValueError(
                    "trying to add multiple column bqdf as single column.")
            colname = content.columns[0]
            tempdf = self.query('SELECT %s as %s from %s' %
                                (colname, column, content.tablename))
            length = len(tempdf)
            newremote = tempdf.remote
        else:
            length = len(self)
            newremote = create_column_from_values(
                self.con, column, content, self.remote, length=length)
        return newremote, length

    def _check_write(self, newremote, timeout=10):
        '''query from a newly created table (waits until table has been fully inserted)'''
        loaded = False
        start_time = time()
        elapsed_time = 0
        while not loaded:
            if elapsed_time < timeout:
                resource = get_table_resource(
                    self.con.client._apiclient, util.dictify(newremote))
                # won't contain this attribute while actively streaming
                # insertions
                if 'numRows' in resource:
                    if int(resource['numRows']) > 0:
                        return True
                elapsed_time = time() - start_time
                sleep(.5)
            else:
                return False

    def _set_active_col(self, col):
        '''sets the "active column" to use for subsequent operation'''
        self.active_col = col
        return self

    def _clear_active_col(self):
        '''clears the active column'''
        self.active_col = None

    def _limit_columns(self, columns, fetch=cfg.FETCH_BY_DEFAULT, dest=None):
        '''create new bqdf limited to these columns)'''
        ndf = self.query('select %s from %s' %
                         (', '.join(columns), self.tablename), fetch=fetch)
        return ndf

    def _simple_agg(self, col=None, operator='COUNT'):
        # TODO figure out nan handling
        if col is None:
            col = self.active_col
        ndf = self.query('SELECT %s(%s) from %s' %
                         (operator, col, self.tablename), fetch=False, fill=False)
        self._clear_active_col()
        return ndf.local.values[0][0]

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

    def _get_nth_row(self, n):
        fields, data = con.client.ReadSchemaAndRows(
            util.dictify(self.remote), start_row=n, max_rows=1)
        result = {f['name']: d for f, d in zip(fields, data[0])}
        return result

    def _add_col_join(self, newdfname, newcol, inplace):
        allcols = [col for col in self.columns + [newcol] if col != 'index']
        querystr = 'SELECT %s FROM (SELECT ROW_NUMBER() OVER() as index, * FROM %s) as tb1 JOIN(SELECT ROW_NUMBER() OVER() as index, * from %s) tb2 on tb1.index==tb2.index' % (
            ', '.join(allcols), newdfname, self.tablename)
        if inplace:
            dest = self.remote
        else:
            dest = None
        print querystr
        df, _, _ = raw_query(self.con, querystr, self.last_modified,
                             dest=dest, overwrite_method='overwrite', fetch=self.fetched)
        return df


##########################################################################
############################################ SUPPLEMENTAL FUNCTIONS ######
##########################################################################

def raw_query(con, querystr, last_modified, dest=None, max_rows=cfg.MAX_ROWS, fetch=cfg.FETCH_BY_DEFAULT, overwrite_method='fail'):
    '''executes a query and returns the results or a result sample as a pandas df and the destination table as a dict

    INPUTS:
        querystr (str):
        dest (dict): specify destination table for output of query (if None, BQ creates a temporary (24hr) table)
        max_rows (int): max number of rows that the con will return in the results
        fetch (bool): if True, fetch the full resultset locally, otherwise return only a sample of the first 5 rows
    OUTPUTS:
        result (pandas dataframe): dataframe containing the query results or
            first 5 rows or resultset (if fetch==True)
        destinationtable (dict): remote table that contains the query results
    '''
    exists = con._check_query(querystr, fetch, last_modified)
    if overwrite_method == 'append':
        write_disposition = 'WRITE_APPEND'
    elif overwrite_method == 'overwrite':
        write_disposition = 'WRITE_TRUNCATE'
    else:
        write_disposition = 'WRITE_EMPTY'
    if not exists:
        query_response = run_query(
            con, querystr, destination_table=dest, write_disposition=write_disposition)
        if fetch:
            fields, data = fetch_query(
                con, query_response, start_row=0, max_rows=max_rows)
            df, source = bqresult_2_df(fields, data), query_response[
                'configuration']['query']['destinationTable']
            con._cache_query(querystr, df, source, fetch)
            if con.client._apiclient.tables().get(**source).execute()['numRows'] > max_rows:
                exceeds_max_rows = True
            else:
                exceeds_max_rows = False
            return df, source, exceeds_max_rows

        else:
            fields, data = fetch_query(
                con, query_response, start_row=0, max_rows=5)
            head_sample = bqresult_2_df(fields, data)
            df, source = head_sample, query_response[
                'configuration']['query']['destinationTable']
            exceeds_max_rows = False
            con._cache_query(querystr, df, source, fetch)
            return df, source, exceeds_max_rows
    else:
        return con._fetch_from_cache(querystr)


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
    return 'SUM(CASE WHEN %s>=%.5f and %s<%.5f THEN 1 ELSE 0 END) as %s' % (col, minq, col, maxq, "%s_%.0f" % (col, maxq))


def _create_case_str_mean(xcol, ycol, minq, maxq, qn):
    return 'AVG(CASE WHEN %s>=%.5f and %s<%.5f THEN %s ELSE NULL END) as %s' % (xcol, minq, xcol, maxq, ycol, "_%.0f" % ((minq + maxq) / 2))


def _create_case_str_std(xcol, ycol, minq, maxq, qn):
    return 'STDDEV(CASE WHEN %s>=%.5f and %s<%.5f THEN %s ELSE NULL END) as %s' % (xcol, minq, xcol, maxq, ycol, "_%.0f" % ((minq + maxq) / 2))


def _create_where_statement(*args):
    operations = ['==', '>', '<', '>=', '<=', '!=',
                  'CONTAINS', 'IN', 'IS NULL', 'IS NOT NULL']
    wheres = []
    for expression in args[0]:
        for o in operations:
            try:
                output = expression.split(o)
                col = output[0].strip()
                try:
                    val = float(output[1].strip())
                except ValueError:
                    val = '"%s"' % output[1].strip()
                wheres.append(_create_single_where(col, val, o))
                break
            except:
                pass
    return ' AND '.join(wheres)


def _create_single_where(key, value, operation):
    return '%s %s %s' % (key, operation, value)
