''' pybq.bqviz: helper functions that support plotting functionality of BQDF
'''

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import itertools


def _get_summaries(plotdf, value_col, grouping_col):
    '''generates labels, means, and sems for plotting'''
    labels = plotdf[grouping_col].values
    means = plotdf[value_col + '_mean'].values
    stds = plotdf[value_col + '_std'].values
    counts = plotdf[value_col + '_count'].values
    sems = [std / np.sqrt(count) for std, count in zip(stds, counts)]
    return labels, means, sems


def _plot_grouped_data(plotdf, value_col, grouping_col, kind='bar', ax=None):
    '''plots data from value_col (Y), grouped by grouping_col (X)'''
    labels, means, sems = _get_summaries(plotdf, value_col, grouping_col)
    if ax is None:
        f, ax = plt.subplots(figsize=[14, 3])
    xaxis = range(len(means))
    if kind == 'line':
        ax.errorbar(xaxis, means, yerr=sems)
    else:
        ax.bar(xaxis, means, yerr=sems)
    ax.set_xlim([min(xaxis), max(xaxis)])
    ax.set_xticklabels(labels, rotation=90)
    return ax


def _gridplot(bqdf):
    plotables = [col for col in bqdf.columns if bqdf.local[
        col].dtype in (np.int64, np.float64) or len(bqdf[col].unique()) < 10]
    f, axes = plt.subplots(len(plotables), len(plotables))
    for col1, col2 in itertools.combinations(plotables, 2):
        i, j = plotables.index(col1), plotables.index(col2)
        axis = axes[i, j]
        if col1 == col2:
            _ = bqdf.hist(col1, bins=20, ax=axis)
        elif bqdf.local[col1].dtype in (np.int64, np.float64) and bqdf.local[col2].dtype in (np.int64, np.float64):
            _ = bqdf.scatter(x=col1, y=col2, bins=200, ax=axis)
        elif bqdf.local[col1].dtype not in (np.int64, np.float64) and bqdf.local[col2].dtype not in (np.int64, np.float64):
            matdf = bqdf.contingency_mat(col1, col2)
            bqdf.plot_matrix(matdf, ax=axis)
        else:
            if bqdf.local[col1].dtype in (np.int64, np.float64):
                _ = bqdf.plot(col2, col1, ax=axis)
            else:
                _ = bqdf.plot(col1, col2, ax=axis)


def plot_matrix(plotdf, axislabels=True, ax=None):
    '''plots a heatmap for a matrix (e.g. for confusion matrices or contingency matrices'''
    if ax is None:
        f, ax = plt.subplots(figsize=[4,4])
    ax.pcolor(plotdf.values, cmap='hot')
    ax.set_yticks(np.arange(len(plotdf.index)) + .5)
    ax.set_xticks(np.arange(len(plotdf.columns)) + .5)
    ax.set_yticklabels(plotdf.index)
    ax.set_xticklabels(plotdf.columns, rotation=90)
    return ax


def plot_hist(df, col, ax=None):
    '''plot histogram of values in col'''
    labels = ['<' + val[1:] for val in df.T.index.values]
    freqs = df.T.iloc[:, 0].values
    if ax is None:
        f, ax = plt.subplots(figsize=[8, 4])
    ax.bar(range(len(freqs)), freqs)
    ax.set_xticklabels(labels, rotation=90)
    ax.set_ylabel('frequency')
    ax.set_title("%s histogram" % col)


def plot_scatter(df, xcol, ycol, ax=None, downsampled=True, error=None, counts=None):
    '''plot a scatter plot of the data in xcol vs. ycol
    INPUTS:
       df (pandas dataframe): contains x and y datapoints to plot
       xcol (str): name of x column
       ycol (str): name of y column
       ax (plt axis): optional axis to plot on
       downsampled (bool): specifies whether we are plotting all the datapoints or summaries of different bins (downsampled=True)
       error (list/array): standard error to plot if plotting downsampled summaries
       counts (list/array): number of datapoints contributing to each marker if plotting downsampled summaries (used to size markers)
    OUPUTS:
       plotdf (pandas dataframe): contains all data used to create plot
    '''
    if ax is None:
        f, ax = plt.subplots(figsize=[8, 4])
    if downsampled:
        x = [float(val[1:]) for val in df.columns]
        y = df.T[0].values
        minc, maxc = min(counts), max(counts)
        normalized_counts = [
            300 * (float((count - minc)) / (maxc - minc)) for count in counts]
        plt.scatter(x, y, marker='o', s=normalized_counts)
        plt.errorbar(x, y, yerr=error, ecolor='#6699ff', linestyle="None")
        ax.set_ylabel(ycol + " +/- SEM")
    else:
        x = df[xcol]
        y = df[ycol]
        ax.scatter(x, y)
        ax.set_ylabel(ycol)
    ax.set_xlabel(xcol)
    if downsampled:
        ax.set_title("downsampled to %s bins" % len(x))
        return pd.DataFrame(index=x, data={'mean_' + ycol: y, 'std_' + ycol: error, 'count' + ycol: counts})
    else:
        ax.set_title("all data shown")
        return pd.DataFrame(index=x, data={ycol: y})
