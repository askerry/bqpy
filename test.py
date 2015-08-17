import unittest

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import scipy.stats
import scipy.spatial.distance as dist

import sys
sys.path.append('/users/amyskerry/documents/projects')
from pybq import bqdf, core
from pybq import credentialscfg as cred

con = bqdf.Connection(project_id=cred.project_id, logging_file=cred.log)
df = bqdf.BQDF(con, 'lomulation:temp.test_data')

testdata = pd.read_csv('data/test_data.csv')[
    ['weight_pounds', 'month', 'day', 'mother_age', 'child_race', 'state']].dropna()
fulldata = pd.read_csv('data/test_data.csv')[
    ['weight_pounds', 'month', 'day', 'mother_age', 'child_race', 'state', 'fat_weights', 'day_20']]
numeric1 = 'weight_pounds'
numeric2 = 'mother_age'
string1 = 'state'
categorical2 = 'month'


def array_allclose(a, b):
    matches = []
    for i, j in zip(a, b):
        try:
            if np.isnan(i) and np.isnan(j):
                matches.append(True)
            else:
                matches.append(np.allclose(i, j))
        except:
            matches.append(np.allclose(i, j))
    return matches


def assertDataFrameMatch(self, df1, df2):
    for col in df2.columns:
        bqans = df1[col].values
        pdans = df2[col].values
        if df2[col].dtype == np.dtype('O'):
            self.assertItemsEqual(bqans, pdans)
        else:
            self.assertTrue(array_allclose(bqans, pdans))


class UnivariateTests(unittest.TestCase):

    def test_mean(self):
        self.assertAlmostEqual(df[numeric1].mean(), testdata[numeric1].mean())

    def test_sum(self):
        self.assertAlmostEqual(df[numeric1].sum(), testdata[numeric1].sum())

    def test_std(self):
        self.assertAlmostEqual(df[numeric1].std(), testdata[numeric1].std())

    def test_sem(self):
        self.assertAlmostEqual(df[numeric1].sem(), testdata[numeric1].sem())

    def test_mode(self):
        self.assertAlmostEqual(
            df[numeric1].mode(), testdata[numeric1].mode()[0])

    def test_max(self):
        self.assertAlmostEqual(df[numeric1].max(), testdata[numeric1].max())

    def test_min(self):
        self.assertAlmostEqual(df[numeric1].min(), testdata[numeric1].min())

    def test_count(self):
        self.assertAlmostEqual(
            df[numeric1].count(), len(testdata[numeric1].dropna()))

    def test_percentiles(self):
        pass


class TransformationTests(unittest.TestCase):

    def test_abs(self):
        bqans = df[numeric1].abs(
            fetch=True).local['abs'].values
        pdans = fulldata[numeric1].abs().values
        self.assertTrue(array_allclose(bqans, pdans))

    def test_sqrt(self):
        bqans = df[numeric1].sqrt(fetch=True).local[
            'sqrt'].values
        pdans = fulldata[numeric1].apply(lambda x: np.sqrt(x)).values
        self.assertTrue(array_allclose(bqans, pdans))

    def test_round(self):
        bqans = df[numeric1].round(dig=2, fetch=True).local[
            'round'].values
        pdans = fulldata[numeric1].apply(lambda x: round(x, 2)).values
        self.assertTrue(array_allclose(bqans, pdans))

    def test_zscore(self):
        mean, std = fulldata[numeric1].mean(), fulldata[numeric1].std()
        bqans = df[numeric1].zscore(fetch=True).local[
            'zscore'].values
        pdans = fulldata[numeric1].apply(lambda x: (x - mean) / std).values
        self.assertTrue(array_allclose(bqans, pdans))

    def test_pow2(self):
        bqans = df[numeric1].pow(power=2, fetch=True).local['pow'].values
        pdans = fulldata[numeric1].pow(2).values
        self.assertTrue(array_allclose(bqans, pdans))

    def test_pow5(self):
        bqans = df[numeric1].pow(power=5, fetch=True).local['pow'].values
        pdans = fulldata[numeric1].pow(5).values
        self.assertTrue(array_allclose(bqans, pdans))

    def test_ln(self):
        bqans = df[numeric1].log(base='e', fetch=True).local[
            'log'].values
        pdans = fulldata[numeric1].apply(lambda x: np.log(x)).values
        self.assertTrue(array_allclose(bqans, pdans))

    def test_log10(self):
        bqans = df[numeric1].log(base=10, fetch=True).local[
            'log'].values
        pdans = fulldata[numeric1].apply(lambda x: np.log10(x)).values
        self.assertTrue(array_allclose(bqans, pdans))


class RelationTests(unittest.TestCase):

    def test_cos(self):
        bqans = df.cos(numeric1, numeric2)
        pdans = 1 - \
            dist.cosine(testdata[numeric1].values, testdata[numeric2].values)
        self.assertAlmostEqual(bqans, pdans)

    def test_euclidean(self):
        bqans = df.euclidean(numeric1, numeric2)
        pdans = dist.euclidean(
            testdata[numeric1].values, testdata[numeric2].values)
        self.assertAlmostEqual(bqans, pdans)

    def test_corr(self):
        bqans = df.corr(numeric1, numeric2),
        pdans = scipy.stats.pearsonr(
            testdata[numeric1].values, testdata[numeric2].values)[0]
        self.assertAlmostEqual(bqans, pdans)

    def test_dot(self):
        bqans = df.dot(numeric1, numeric2)
        pdans = testdata[numeric1].dot(testdata[numeric2])
        self.assertAlmostEqual(bqans, pdans)

    def contingency_mat(self):
        pass


class SeriesTests(unittest.TestCase):

    def test_subtract(self):
        bqans = df.subtract(numeric1, numeric2, fetch=True).local[
            'diff'].values
        pdans = fulldata[numeric1] - fulldata[numeric2]
        self.assertTrue(array_allclose(bqans, pdans))

    def test_add(self):
        bqans = df.add(numeric1, numeric2, fetch=True).local[
            'sum'].values
        pdans = fulldata[numeric1] + fulldata[numeric2]
        self.assertTrue(array_allclose(bqans, pdans))

    def test_divide(self):
        bqans = df.divide(numeric1, numeric2, fetch=True).local[
            'div'].values
        pdans = fulldata[numeric1] / fulldata[numeric2]
        self.assertTrue(array_allclose(bqans, pdans))

    def test_multiply(self):
        bqans = df.multiply(numeric1, numeric2, fetch=True).local[
            'product'].values
        pdans = fulldata[numeric1] * fulldata[numeric2]
        self.assertTrue(array_allclose(bqans, pdans))

    def test_lower(self):
        bqans = df[string1].lower(
            fetch=True).local['lower'].values
        pdans = fulldata[string1].apply(lambda x: x.lower()).values
        self.assertSequenceEqual(list(bqans), list(pdans))

    def test_upper(self):
        bqans = df[string1].upper(fetch=True).local[
            'upper'].values
        pdans = fulldata[string1].apply(lambda x: x.upper()).values
        self.assertSequenceEqual(list(bqans), list(pdans))

    def test_replace_str(self):
        bqans = df[string1].replace_str('A', 'XXXX', fetch=True).local[
            'replace'].values
        pdans = fulldata[string1].apply(
            lambda x: x.replace('A', 'XXXX')).values
        self.assertSequenceEqual(list(bqans), list(pdans))

    def test_str_index(self):
        bqans = df[string1].str_index('A', fetch=True).local[
            'position'].values
        pdans = fulldata[string1].apply(lambda x: x.index('A')).values
        # self.assertSequenceEqual(list(bqans), list(pdans))
        pass


class TableOperations(unittest.TestCase):

    def test_where_equality(self):
        bqans = df.where('%s == MA' % (
            string1), fetch=True).local
        pdans = fulldata[fulldata[string1] == 'MA']
        assertDataFrameMatch(self, bqans, pdans)

    def test_where_comparison(self):
        bqans = df.where('%s < 10' % (numeric1
                                      ), fetch=True).local
        pdans = fulldata[fulldata[numeric1] < 10]
        assertDataFrameMatch(self, bqans, pdans)

    def test_where_multi(self):
        bqans = df.where('%s < 10' % (numeric1
                                      ), '%s == CA' % (string1), fetch=True).local
        pdans = fulldata[
            (fulldata[numeric1] < 10) & (fulldata[string1] == 'CA')]
        assertDataFrameMatch(self, bqans, pdans)

    def test_groupby_mean(self):
        bqans = df.groupby(string1, [(numeric1, 'mean')])
        pdans = testdata.groupby(string1)[[numeric1]].mean().reset_index()

    def test_groupby_sem(self):
        bqans = df.groupby(string1, [(numeric1, 'std')])
        pdans = testdata.groupby(string1)[[numeric1]].std().reset_index()
        assertDataFrameMatch(self, bqans, pdans)

    def test_groupby_count(self):
        bqans = df.groupby(string1, [(numeric1, 'count')])
        pdans = testdata.groupby(string1)[[numeric1]].count().reset_index()
        assertDataFrameMatch(self, bqans, pdans)

    def test_groupby_min(self):
        bqans = df.groupby(string1, [(numeric1, 'min')])
        pdans = testdata.groupby(string1)[[numeric1]].min().reset_index()
        assertDataFrameMatch(self, bqans, pdans)

    def test_groupby_apply(self):
        pass

    def test_apply(self):
        pass

    def test_join(self):
        pass

    def test_add_index(self):
        pass

    def test_dropna(self):
        pass

    def test_split_unstack(self):
        pass

    def test_sort_by(self):
        pass

    def test_slice(self):
        pass

    def test_replace(self):
        pass

    def test_add_col(self):
        pass

    def test_rename(self):
        pass


class StatisticalTests(unittest.TestCase):

    def test_ttest_1samp(self):
        t, pval = scipy.stats.ttest_1samp(testdata[numeric1].values, 9.5)
        degfree = len(testdata[numeric1].dropna()) - 1
        pdans = (t, degfree, pval)
        bqans = df.ttest_1samp(numeric1, 9.5)
        self.assertSequenceEqual(bqans, pdans)

    def test_ttest_ind(self):
        bqans = df.ttest_ind(numeric1, numeric2)
        t, pval = scipy.stats.ttest_ind(
            testdata[numeric1].values, testdata[numeric2].values)
        degfree = len(testdata[numeric1].dropna()) + \
            len(testdata[numeric2].dropna()) - 2
        pdans = (t, degfree, pval)
        self.assertSequenceEqual(bqans, pdans)

    def test_ttest_rel(self):
        bqans = df.ttest_rel(numeric1, numeric2)
        r, pval = scipy.stats.ttest_rel(
            testdata[numeric1].values, testdata[numeric2].values)
        degfree = len(testdata[numeric1].dropna()) - 1
        pdans = (r, degfree, pval)
        self.assertSequenceEqual(bqans, pdans)

    def test_chi_square(self):
        pass

    def test_binomial(self):
        pass

    def test_pearsonr(self):
        bqans = df.ttest_rel(numeric1, numeric2)
        r, pval = scipy.stats.ttest_rel(
            testdata[numeric1].values, testdata[numeric2].values)
        degfree = len(testdata[numeric1].dropna()) - 1
        pdans = (r, degfree, pval)
        self.assertSequenceEqual(bqans, pdans)

    def test_onewayanova(self):
        pass

    def test_twowayanova(self):
        pass

    def test_rmanova(self):
        pass

    def test_linear_regression(self):
        pass


class SummarizationTests():
    # def test_describe(self):
    #    pass
    # hist values
    # columns

    def test_columns(self):
        self.assertTrue(
            array_allclose(df[numeric1].columns, fulldata[numeric1].columns))
    # limiting

    def test_unique(self):
        self.assertTrue(array_allclose(
            df[numeric1].unique(fetch=True), fulldata[numeric1].unique().values))

    def test_values(self):
        pass

    def test_head(self):
        pass

    def test_size(self):
        pass


if __name__ == '__main__':
    unittest.main()
