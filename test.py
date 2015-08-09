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
df = bqdf.BQDF(con, 'lomulation:temp.stats_reference')

testdata = pd.read_csv('stats_reference.csv')[
    ['weight_pounds', 'month', 'day', 'mother_age', 'state']].dropna()
fulldata = pd.read_csv('stats_reference.csv')[
    ['weight_pounds', 'month', 'day', 'mother_age', 'state']]
numeric1 = 'weight_pounds'
numeric2 = 'mother_age'
categorical = 'state'


def df_allclose(df1, df2):
    colmatch = df1.columns == df2.columns
    valmatch = array_allclose(df1.values.flatten, df1.values.flatten)
    return colmatch & valmatch


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

    # bqdf.median not implemented
    # def test_median(self):
    #    self.assertAlmostEqual(df[numeric1].median(), testdata[numeric1].median())

    def test_max(self):
        self.assertAlmostEqual(df[numeric1].max(), testdata[numeric1].max())

    def test_min(self):
        self.assertAlmostEqual(df[numeric1].min(), testdata[numeric1].min())

    def test_count(self):
        self.assertAlmostEqual(
            df[numeric1].count(), len(testdata[numeric1].dropna()))


class TransformationTests(unittest.TestCase):

    def test_abs(self):
        self.assertTrue(array_allclose(df[numeric1].abs(
            fetch=True).local['abs'].values, fulldata[numeric1].abs().values))

    def test_pow2(self):
        self.assertTrue(array_allclose(
            df[numeric1].pow(power=2, fetch=True).local['pow'].values, fulldata[numeric1].pow(2).values))

    def test_pow5(self):
        self.assertTrue(array_allclose(
            df[numeric1].pow(power=5, fetch=True).local['pow'].values, fulldata[numeric1].pow(5).values))

    def test_ln(self):
        self.assertTrue(array_allclose(df[numeric1].log(base='e', fetch=True).local[
                        'log'].values, fulldata[numeric1].apply(lambda x: np.log(x)).values))

    def test_log10(self):
        self.assertTrue(array_allclose(df[numeric1].log(base=10, fetch=True).local[
                        'log'].values, fulldata[numeric1].apply(lambda x: np.log10(x)).values))


class RelationTests(unittest.TestCase):

    def test_cos(self):
        self.assertAlmostEqual(df.cos(numeric1, numeric2), 1 -
                               dist.cosine(testdata[numeric1].values, testdata[numeric2].values))

    def test_euclidean(self):
        self.assertAlmostEqual(df.euclidean(numeric1, numeric2),
                               dist.euclidean(testdata[numeric1].values, testdata[numeric2].values))

    def test_corr(self):
        self.assertAlmostEqual(df.corr(numeric1, numeric2), scipy.stats.pearsonr(
            testdata[numeric1].values, testdata[numeric2].values)[0])


class AggregationTests():
    # groupby
    pass


class SummarizationTests():
    # def test_describe(self):
    #    pass
    # hist values
    # columns

    def test_column(self):
        self.assertTrue(
            array_allclose(df[numeric1].columns, fulldata[numeric1].columns))
    # limiting

    def test_unique(self):
        self.assertTrue(array_allclose(
            df[numeric1].unique(fetch=True), fulldata[numeric1].unique().values))
    # corr mat
    pass


if __name__ == '__main__':
    unittest.main()
